create_job_schema = {
    "type": "object",
    "properties": {
        "job_name": {"type": "string", "minLength": 5},
        "args": {"type": "object", "minProperties": 1},
    },
    "required": ["job_name", "args"]
}
