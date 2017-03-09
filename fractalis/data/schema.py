create_data_schema = {
    "type": "object",
    "properties": {
        "handler": {"type": "string"},
        "server": {"type": "string"},
        "auth": {
            "type": "object",
            "properties": {
                "token": {"type": "string"},
                "user": {"type": "string"},
                "passwd": {"type": "string"}
            },
            "minProperties": 1
        },
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
    "required": ["handler", "server", "auth", "descriptors"]
}
