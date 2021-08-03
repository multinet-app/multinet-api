from django.contrib.auth.models import User
import factory
import factory.fuzzy

from multinet.api.models import Network, Table, Workspace


class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = User

    username = factory.SelfAttribute('email')
    email = factory.Faker('safe_email')
    first_name = factory.Faker('first_name')
    last_name = factory.Faker('last_name')


class PrivateWorkspaceFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workspace

    name = factory.fuzzy.FuzzyText()


class PublicWorkspaceFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workspace

    name = factory.fuzzy.FuzzyText()
    public = True


class NetworkFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Network

    name = factory.fuzzy.FuzzyText()
    workspace = factory.SubFactory(PrivateWorkspaceFactory)


class EdgeTableFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Table

    name = factory.fuzzy.FuzzyText()
    workspace = factory.SubFactory(PrivateWorkspaceFactory)
    edge = True


class NodeTableFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Table

    name = factory.fuzzy.FuzzyText()
    workspace = factory.SubFactory(PrivateWorkspaceFactory)


# Default table to node table
class TableFactory(NodeTableFactory):
    pass
