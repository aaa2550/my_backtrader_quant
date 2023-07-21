import os

# Get the user's home directory
home_directory = os.path.expanduser("~")

# Concatenate the rest of the path to the home directory
relative_path = 'stock/cache'
pers_path = os.path.join(home_directory, relative_path)

print(pers_path)