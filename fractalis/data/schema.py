create_data_schema = {
    "type": "object",
    "properties": {
        "handler": {"type": "string"},
        "server": {"type": "string"},
        "token": {"type": "string"},
        "descriptors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "data_type": {"type": "string"},
                },
                "minProperties": 2,
                "required": ["data_type"]
            }
        }
    },
    "required": ["handler", "server", "token", "descriptors"]
}
