# The class is saved in a separate file to avoid pickling issues
class SerializableTokenizer:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    def __call__(self, text):
        return self.tokenizer.encode(text).tokens