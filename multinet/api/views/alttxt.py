import json
from typing import Union

from alttxt.enums import AggregateBy, Explanation, Level, Verbosity
from alttxt.generator import AltTxtGen
from alttxt.models import DataModel, GrammarModel
from alttxt.parser import Parser
from alttxt.tokenmap import TokenMap
from django.core.files.uploadedfile import UploadedFile
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers
from rest_framework.parsers import MultiPartParser
from rest_framework.views import APIView


class AlttxtSerializer(serializers.Serializer):
    verbosity = serializers.ChoiceField(choices=Verbosity.list())
    level = serializers.ChoiceField(choices=Level.list())
    explain = serializers.ChoiceField(choices=Explanation.list())
    title = serializers.CharField(max_length=200, default='')
    data = serializers.FileField(allow_empty_file=False)


class UpsetAltTextGenerate(APIView):
    authentication_classes = []
    permission_classes = []
    parser_classes = [MultiPartParser]

    @swagger_auto_schema(request_body=AlttxtSerializer)
    def post(self, request) -> HttpResponse:
        """
        Take params and returns an alttxt.

        Process and respond to a form-multipart request
        containing the params and data needed to generate an alttxt.
        Returns an alttxt in JSON format if successful, otherwise,
        returns a descriptive 500 error message.
        """
        # Get data and serialize it
        serializer = AlttxtSerializer(data=request.data)
        if not serializer.is_valid():
            return HttpResponseBadRequest('Error validating fields: ' + str(serializer.errors))

        # Fields into variables
        parsed_data = serializer.validated_data
        verbosity: Verbosity = Verbosity(parsed_data['verbosity'])
        level: Level = Level(parsed_data['level'])
        explain: Explanation = Explanation(parsed_data['explain'])
        title: str = parsed_data['title']
        datafile: Union[UploadedFile, str] = parsed_data['data']

        # Load the data
        try:
            if isinstance(datafile, UploadedFile):
                data: dict = json.loads(datafile.read())
            elif isinstance(datafile, str):
                data: dict = json.loads(datafile)
            else:
                return HttpResponseBadRequest('Invalid data file: must' ' be a JSON file or string')

        except json.decoder.JSONDecodeError as e:
            return HttpResponseBadRequest('Invalid JSON: ' f'error while parsing: {e.msg}')

        # Validate the data
        try:
            if (
                not isinstance(data, dict)
                or 'firstAggregateBy' not in data
                or AggregateBy(data['firstAggregateBy']) != AggregateBy.NONE
            ):
                return HttpResponseBadRequest(
                    'Invalid data file: JSON' ' must be aggregated by NONE'
                )
        except ValueError:
            return HttpResponseBadRequest(
                'Invalid data file: '
                f"{data['firstAggregateBy']} is not "
                'a valid aggregation type'
            )

        # Now parse & generate the alttxt
        parser: Parser = Parser(data)
        try:
            grammar: GrammarModel = parser.get_grammar()
            data: DataModel = parser.get_data()
        except ValueError as e:
            return HttpResponseBadRequest('Error while parsing data: ' + str(e))

        tokenmap: TokenMap = TokenMap(data, grammar, title)
        generator: AltTxtGen = AltTxtGen(level, verbosity, explain, tokenmap, grammar)

        return JsonResponse({'alttxt': generator.text})
