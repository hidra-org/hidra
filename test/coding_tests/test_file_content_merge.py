import copy
import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))

if __name__ == "__main__":
    filename = os.path.join(BASE_DIR, "test", "test_files", "test_file.cbf")
    chunksize = 10485760

    data_chunks = []
    with open(filename) as f:
        while True:
            data = f.read(chunksize)
            data_chunks.append(copy.deepcopy(data))

            if not data:
                break

    print(len(data_chunks))

    with open(filename) as f:
        all_data = f.read()

    if all_data == "".join(data_chunks):
        print("Is the same")
    else:
        print("Is NOT the same")
