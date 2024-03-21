import os

def load_text( filename):
    try:
        with open(filename, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print("File not found")
        print(os.getcwd())
        return None
