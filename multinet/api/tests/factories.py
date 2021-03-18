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


class WorkspaceFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Workspace

    name = factory.fuzzy.FuzzyText()


class NetworkFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Network

    name = factory.fuzzy.FuzzyText()
    workspace = factory.SubFactory(WorkspaceFactory)


class TableFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Table

    name = factory.fuzzy.FuzzyText()
    workspace = factory.SubFactory(WorkspaceFactory)
