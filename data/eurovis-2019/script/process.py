"""Download Les Miserables data from the web and process it into Multinet CSV files."""
import csv
import json
import sys


def add_key(rec, idx):
    """Add a key value to the character records."""
    rec['_key'] = rec['id']

    del rec['utc_offset']
    del rec['id']
    del rec['memberFor_days']
    del rec['neighbors']
    del rec['edges']
    del rec['userSelectedNeighbors']
    del rec['selected']
    del rec['original']
    del rec['memberSince']

    return rec


def convert_link(link, idx):
    """Convert the D3 JSON link data into a Multinet-style record."""
    link['_key'] = str(idx)
    link['_from'] =  f"people/{link['source']}"
    link['_to'] =  f"people/{link['target']}"

    del link['id']
    del link['source']
    del link['target']
    del link['selected']

    return link


def write_csv(data, fields, filename):
    """Write a CSV file from data and field names."""
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fields)

        writer.writeheader()
        for row in data:
            writer.writerow(row)


def main():
    """Run main function."""
    data = json.loads(sys.stdin.read())

    # Prepare the node data by adjoining a key value equal to each record's
    # index in the original data.
    nodes = [add_key(record, index) for (index, record) in enumerate(data['nodes'])]

    # Convert the link data to Multinet form. Note that the D3 JSON format uses
    # node list indices to refer to the source and target nodes; these can be
    # used unchanged because of how the key value for the nodes was set above.
    links = [convert_link(link, index) for (index, link) in enumerate(data['links'])]

    # Filter links to those with both in node table
    links = [
        link
        for link in links
        if (
            any(f"people/{node['_key']}" == link['_from'] for node in nodes)
            and any(f"people/{node['_key']}" == link['_to'] for node in nodes)
        )
    ]

    # Write out both the node and link data to CSV files.
    write_csv(
        nodes,
        nodes[0].keys(),
        'people.csv',
    )
    write_csv(links, links[0].keys(), 'connections.csv')

    return 0


if __name__ == '__main__':
    sys.exit(main())
