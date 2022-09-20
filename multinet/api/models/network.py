from __future__ import annotations

from typing import List, Type

from arango.cursor import Cursor
from arango.graph import Graph
from django.db import models
from django.db.models.signals import post_delete, pre_save
from django.dispatch.dispatcher import receiver
from django_extensions.db.models import TimeStampedModel

from multinet.api.utils.arango import ArangoQuery

from .workspace import Workspace

import time


class Network(TimeStampedModel):
    name = models.CharField(max_length=300)
    workspace = models.ForeignKey(Workspace, related_name='networks', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('workspace', 'name')

    @property
    def node_count(self):
        db = self.workspace.get_arango_db()
        return sum(
            db.collection(coll).count() for coll in self.get_arango_graph().vertex_collections()
        )

    def nodes(self, limit: int = 0, offset: int = 0) -> Cursor:
        return (
            ArangoQuery.from_collections(self.workspace.get_arango_db(), self.node_tables())
            .paginate(limit=limit, offset=offset)
            .execute()
        )

    @property
    def edge_count(self) -> int:
        db = self.workspace.get_arango_db()
        return sum(
            db.collection(edge_def['edge_collection']).count()
            for edge_def in self.get_arango_graph().edge_definitions()
        )

    def edges(self, limit: int = 0, offset: int = 0) -> Cursor:
        return (
            ArangoQuery.from_collections(self.workspace.get_arango_db(), self.edge_tables())
            .paginate(limit=limit, offset=offset)
            .execute()
        )

    def get_arango_graph(self) -> Graph:
        workspace: Workspace = self.workspace
        return workspace.get_arango_db().graph(self.name)

    def node_tables(self) -> List[str]:
        return self.get_arango_graph().vertex_collections()

    def edge_tables(self) -> List[str]:
        return [
            edge_def['edge_collection'] for edge_def in self.get_arango_graph().edge_definitions()
        ]

  
    @classmethod
    def create_with_edge_definition(
        cls, name: str, workspace: Workspace, edge_table: str, node_tables: List[str], run_graph_analyses=True, 
        calculate_degree=True) -> Network:
        """Create a network with an edge definition, using the provided arguments."""
        # Create graph in arango before creating the Network object here
        workspace.get_arango_db(readonly=False).create_graph(
            name,
            edge_definitions=[
                {
                    'edge_collection': edge_table,
                    'from_vertex_collections': node_tables,
                    'to_vertex_collections': node_tables,
                }
            ],
        )

        # default behavior is to run Pregel algorithms when a new network is created.  This stores 
        # data back in the node table.  If there is no node table, prohibit running the algorithms
        if run_graph_analyses and (len(node_tables)>0):

            try:
                # automatically add graph measurements to the graph by running analysis jobs
                arango_db = workspace.get_arango_db(readonly=False)
                print("running page rank analysis on:",name)
                pagerank_job_id = arango_db.pregel.create_job(
                    graph=name,
                    algorithm='pagerank',
                    store=True,
                    #maxx_gss=100,
                    #thread_count=1,
                    async_mode=False,
                    result_field='_pagerank'
                )
                # a conflict writing results were noticed when multiple jobs finish about the same time, so 
                # wait after each job before starting the next one

                while ((arango_db.pregel.job(pagerank_job_id)['state'] == 'running') or 
                       (arango_db.pregel.job(pagerank_job_id)['state'] == 'storing')):
                    time.sleep(0.25)
                    print('waiting for pagerank job to finish')

                print("running betweeenness analysis on:",name)
                betweenness_job_id = arango_db.pregel.create_job(
                    graph=name,
                    algorithm='linerank',
                    store=True,
                    async_mode=False,
                    result_field='_betweenness'
                )
                while ((arango_db.pregel.job(betweenness_job_id)['state'] == 'running') or 
                       (arango_db.pregel.job(betweenness_job_id)['state'] == 'storing')):
                    time.sleep(0.25)
                    print('waiting for betweenness job to finish')


                print("running label propogation on:",name)
                label_prop_job_id = arango_db.pregel.create_job(
                    graph=name,
                    algorithm='labelpropagation',
                    store=True,
                    async_mode=False,
                    result_field='_community_LP'
                )
                while ((arango_db.pregel.job(label_prop_job_id)['state'] == 'running') or
                       (arango_db.pregel.job(label_prop_job_id)['state'] == 'storing')):
                    time.sleep(0.25)
                    print('waiting for community label propogation job to finish')

                print("running speaker-Listener label propogation on:",name)
                slpa_job_id = arango_db.pregel.create_job(
                    graph=name,
                    algorithm='slpa',
                    store=True,
                    async_mode=False,
                    result_field='_community_SLPA'
                )
                while ((arango_db.pregel.job(slpa_job_id)['state'] == 'running') or 
                       (arango_db.pregel.job(slpa_job_id)['state'] == 'storing')):
                    time.sleep(0.25)
                    print('waiting for SLPA community job to finish')
            except:
                print('error running network analysis via Pregel on network:',name)

            # **TODO Update Django models for pagerank, community, betweenness here.  The Pregel jobs added
            # columns to the Tables without updating the Django models corresponding to these tables.  Here we need to 
            # update the Django Table and the TableTypeAnnotation models so the new attributes are assigned the correct type 
            # when the multinet client applications read the node tables.

        # if calculating the degree automatically has been selected, run an AQL query for each node table. results are written
        # back to each node table as an extra attribute "_degree". overall node degree is calculated, not IN or OUT degree. 
        # To change to calculate IN or OUT degree, the keyword ANY below would be replaced below with INBOUND or OUTBOUND, respectively.             
        if calculate_degree:
            try:
                # iterate through the node tables and calculate the node degree by counting the results discovered 
                # from each 1-hop traversal.  This process has to be repeated for each node_table because of 
                # limitations in AQL (or our understanding of AQL).  We understand an AQL query can only work 
                # over a single collection at a time.  So this loop below repeats the query for each node_table. 

                for collName in node_tables:
                    print('calculating degree for nodes in',collName)
                    query_str = """FOR doc in @@COLL
                            UPDATE {"_key": doc._key,
                            "_degree" : LENGTH(for edge 
                                in 1 ANY doc._id 
                                graph @graphName
                                return edge._id
                                )
                            } in @@COLL
                        RETURN doc._id """
                    # set the node collection and graph name dynamically
                    bind_vars = {'@COLL': collName, 'graphName': name}

                    cursor = arango_db.aql.execute(query=query_str, bind_vars=bind_vars)
            except:
                print('error auto-calculating node degree on network:',name)
                print('AQL attempted was:')
                print(query_str)
                print('bind variables were: @COLL:',collName, 'graphName:',name)

            # **TODO Update Django models for Node degree here.  The  AQL query added
            # columns to the Tables without updating the Django models corresponding to these tables.  Here we need to 
            # update the Django Table and the TableTypeAnnotation models so the new attributes are assigned the correct type 
            # when the multinet client applications read the node tables. 

        return Network.objects.create(
            name=name,
            workspace=workspace,
        )


    def __str__(self) -> str:
        return self.name


# Handle arango sync
@receiver(pre_save, sender=Network)
def arango_graph_save(sender: Type[Network], instance: Network, **kwargs):
    workspace: Workspace = instance.workspace

    db = workspace.get_arango_db(readonly=False)
    if not db.has_graph(instance.name):
        db.create_graph(instance.name)


@receiver(post_delete, sender=Network)
def arango_graph_delete(sender: Type[Network], instance: Network, **kwargs):
    workspace: Workspace = instance.workspace

    db = workspace.get_arango_db(readonly=False)
    if db.has_graph(instance.name):
        db.delete_graph(instance.name)
