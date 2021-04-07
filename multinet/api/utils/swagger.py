from drf_yasg.inspectors import SwaggerAutoSchema

VIEWSET_TAGS_FIELD = 'swagger_tags'


class ImprovedAutoSchema(SwaggerAutoSchema):
    def get_tags(self, operation_keys=None):
        tags = self.overrides.get('tags') or getattr(self.view, VIEWSET_TAGS_FIELD, [])
        if not tags and operation_keys:
            tags = [operation_keys[0]]

        return tags
