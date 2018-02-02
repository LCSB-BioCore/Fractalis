request_state_access_schema = {
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
        }
    },
    "required": ["handler", "server", "auth"]
}
