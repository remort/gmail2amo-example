import pickle

from abc import ABC, abstractmethod


class ClassificationModel(ABC):
    @abstractmethod
    def get_prediction(self, message_text):
        pass


class RandomProbability(ClassificationModel):
    def get_prediction(self, message_text):
        return 0.5


class SGDClassificator(ClassificationModel):
    def __init__(self):
        self.model = self.load_model()
        self.transformer = self.load_transformer()

    @staticmethod
    def load_model():
        with open('sgdc_model.pickle', 'rb') as f:
            return pickle.load(f)

    @staticmethod
    def load_transformer():
        with open('tfidf.pickle', 'rb') as f:
            return pickle.load(f)

    def transform_message(self, message_text):
        return self.transformer.transform([message_text])

    def get_prediction(self, message_text):
        transformed_text = self.transform_message(message_text)
        return self.model.predict(transformed_text)[0]


if __name__ == '__main__':
    clf = SGDClassificator()

