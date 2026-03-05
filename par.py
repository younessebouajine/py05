class InvalidValue(Exception):
    pass


def parsing_line(line: str) -> tuple:
    line = line.strip()
    if "=" not in line:
        raise ValueError(f"Invalid line: {line}")

    key, value = line.split("=", 1)
    key = key.upper().strip()
    value = value.strip()

    if key in ("WIDTH", "HEIGHT"):
        if not value.isdigit():
            raise ValueError(f"{key} must be an integer. Got: {value}")
        value = int(value)
        if value < 10 or value > 20:
            raise InvalidValue(
                f"{key} must be between 10 and 20. Got: {value}"
            )
    elif key in ("ENTRY", "EXIT"):
        parts = value.split(",")
        if len(parts) != 2 or not all(p.strip().isdigit() for p in parts):
            raise ValueError(f"{key} must be in format x,y. Got: {value}")
        value = tuple(int(p.strip()) for p in parts)
    elif key == "PERFECT":
        value = value.lower()
        if value not in ("true", "false"):
            raise ValueError(f"PERFECT must be True or False. Got: {value}")
        value = value == "true"
    elif key == "SEED":
        if not value.isdigit():
            raise ValueError(f"SEED must be an integer. Got: {value}")
        value = int(value)
    elif key == "OUTPUT_FILE":
        if value == "/":
            raise InvalidValue("OUTPUT_FILE cannot be '/'")

    if key not in {"WIDTH", "HEIGHT", "ENTRY", "EXIT", "OUTPUT_FILE", "PERFECT", "SEED"}:
        raise InvalidValue(f"Unknown key: {key}")

    return key, value


def validate_config(config: dict):
    required_keys = {
        "WIDTH", "HEIGHT", "ENTRY",
        "EXIT", "OUTPUT_FILE",
        "PERFECT",
    }

    missing = required_keys - config.keys()
    if missing:
        raise ValueError(f"Missing configuration keys: {missing}")

    if config["WIDTH"] <= 0 or config["HEIGHT"] <= 0:
        raise ValueError("WIDTH and HEIGHT must be greater than 0")

    x_entry, y_entry = config["ENTRY"]
    x_exit, y_exit = config["EXIT"]

    if not (0 <= x_entry < config["WIDTH"] and 0 <= y_entry < config["HEIGHT"]):
        raise ValueError("ENTRY is outside maze bounds")

    if not (0 <= x_exit < config["WIDTH"] and 0 <= y_exit < config["HEIGHT"]):
        raise ValueError("EXIT is outside maze bounds")


def read_file(path="config.txt"):
    config = {}

    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = parsing_line(line)
            if key not in config:
                config[key] = value

    validate_config(config)
    return config
