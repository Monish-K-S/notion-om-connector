from metadata.generated.schema.entity.data.table import Column, DataType

NOTION_TO_OM_MAP = {
    "title": DataType.STRING,
    "rich_text": DataType.TEXT,
    "number": DataType.DOUBLE,
    "select": DataType.ENUM,
    "multi_select": DataType.ARRAY,
    "date": DataType.DATETIME,
    "checkbox": DataType.BOOLEAN,
    "url": DataType.STRING,
    "email": DataType.STRING,
    "phone_number": DataType.STRING,
    "relation": DataType.STRING, 
}

def map_notion_property_to_column(prop_name: str, prop_data: dict) -> Column:
    notion_type = prop_data.get("type")
    om_type = NOTION_TO_OM_MAP.get(notion_type, DataType.STRING)

    return Column(
        name=prop_name,
        displayName=prop_name,
        dataType=om_type,
        description=f"Source Notion Type: {notion_type.upper()}"
    )



