import os
import pickle


def read(file_path):
    file = open(file_path, "r")
    content = file.read()
    file.close()
    return content


def write(content, file_path):
    file = open(file_path, "w")
    file.write(content)
    file.close()


def format(time):
    return time.strftime('%Y-%m-%d')


def do_percent(index, count, key: str = ""):
    print(f"{key}:{round(index / count * 100, 2)}%")


def serialize_data(data, file_path):
    # Create the directory if it doesn't exist
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Serialize data to the specified file
    with open(file_path, 'wb') as file:
        pickle.dump(data, file)

# 从本地文件加载数据并反序列化为字典
def deserialize_data(file_path):
    if not os.path.exists(file_path):
        return None
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data
