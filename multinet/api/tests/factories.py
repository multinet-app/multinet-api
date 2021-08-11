from django.contrib.auth.models import User
import factory
import factory.fuzzy

from multinet.api.models import Network, Table, Workspace
from multinet.api.models.upload import Upload


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
    owner = factory.LazyAttribute(lambda _: UserFactory())
    # owner = factory.SubFactory(UserFactory)


class PublicWorkspaceFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workspace

    name = factory.fuzzy.FuzzyText()
    # owner = factory.SubFactory(UserFactory, first_name='owner')
    owner = factory.LazyAttribute(lambda _: UserFactory())
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


class UploadFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Upload

    workspace = factory.SubFactory(PrivateWorkspaceFactory)
    user = factory.SubFactory(UserFactory)
