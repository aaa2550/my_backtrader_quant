class Triple:

    def __init__(self, left, middle, right):
        self.left = left
        self.middle = middle
        self.right = right

    def __repr__(self):
        return f"Triple(left={self.left}, middle={self.middle}, right={self.right})"