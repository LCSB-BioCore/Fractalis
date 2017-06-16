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
                "minProperties": 1
            }
        }
    },
    "required": ["handler", "server", "auth", "descriptors"]
}
