def load(file):

    if file is None:
        return
    with open("files/"+file, "w") as f:
        f.write(file)


