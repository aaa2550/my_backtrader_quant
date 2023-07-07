def read(file_path):
    file = open(file_path, "r")
    content = file.read()
    file.close()
    return content


def write(content, file_path):
    file = open(file_path, "w")
    file.write(content)
    file.close()


def do_percent(index, count, key: str = ""):
    print(f"{key}:{round(index / count * 100, 2)}%")
