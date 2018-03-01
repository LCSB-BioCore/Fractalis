save_state_schema = {
    "type": "object",
    "properties": {
        "state": {"type": "object"},
        "handler": {"type": "string"},
        "server": {"type": "string"}
    },
    "required": ["handler", "server", "state"]
}


request_state_access_schema = {
    "type": "object",
    "properties": {
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
    "required": ["auth"]
}
