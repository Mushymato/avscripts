import os
import sys
import urllib.request
import random
import json

if __name__ == "__main__":
    directory = sys.argv[1]
    word_url = "https://www.mit.edu/~ecprice/wordlist.10000"
    response = urllib.request.urlopen(word_url)
    long_txt = response.read().decode()
    words = long_txt.splitlines()
    for filename in os.listdir(directory):
        ext = os.path.splitext(filename)[1]
        if ext != ".json":
            continue
        filepath = os.path.join(directory, filename)
        selected_words = [random.choice(words) for _ in range(random.randint(4, 7))]

        with open(filepath, "r") as fn:
            data = json.load(fn)
        data["title"] = " ".join(selected_words)
        with open(filepath, "w") as fn:
            json.dump(data, fn)

        os.rename(os.path.join(directory, filename), os.path.join(directory, data["title"] + ext))