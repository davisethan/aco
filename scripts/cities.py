import sys
from math import sqrt


def find_index(char):
    if "A" <= char <= "Z":
        return ord(char) - ord("A")
    elif "0" <= char <= "9":
        return ord(char) - ord("0") + 26
    else:
        raise ValueError("Usage: [A-Z][0-9]")


def generate_graph(pheromone):
    # read cities
    cities = list()
    with open("cities.txt", "r") as file:
        for line in file:
            city, x, y = line.split("\t")
            cities.append((find_index(city), int(x), int(y)))

    # write cities
    with open("graph.txt", "w") as file:
        for source, x1, y1 in cities:
            for target, x2, y2 in cities:
                if source == target:
                    continue
                weight = sqrt((x1 - x2)**2 + (y1 - y2)**2)
                line = f"{source},{target},{weight},{pheromone}\n"
                file.write(line)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: cities.py <pheromone-start>")
        sys.exit(1)
    pheromone = int(sys.argv[1])
    generate_graph(pheromone)