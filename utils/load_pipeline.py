from utils.utils import SerializableTokenizer
import pandas as pd
import joblib

class Prediction:
    def __init__(self, subjects, subjects_df):
        self.subjects = [subject for subject in subjects]
        self.subjects_df = subjects_df
    
    def get_subjects(self):
        return self.subjects

    def subject_to_full_name(self, subject):
        return self.subjects_df[self.subjects_df["abbreviation"] == subject]["subject_area"].values[0]
    
    def subject_to_supergroup(self, subject):
        return self.subjects_df[self.subjects_df["abbreviation"] == subject]["supergroup"].values[0]
    
    def get_full_names(self):
        return [self.subject_to_full_name(subject) for subject in self.subjects]
    
    def get_supergroups(self):
        return list({self.subject_to_supergroup(subject) for subject in self.subjects})

class Pipeline:
    def __init__(self, pipeline_path, subjects_path):
        pipeline = joblib.load(pipeline_path)
        self.tfidf = pipeline["tfidf"]
        self.mlb = pipeline["mlb"]
        self.model = pipeline["model"]

        self.subjects_df = pd.read_csv(subjects_path)
    
    def predict(self, texts):
        X_predict = self.tfidf.transform(texts)
        y_predict = self.model.predict(X_predict)
        predicted_subjects = self.mlb.inverse_transform(y_predict)
        return [Prediction(subjects, self.subjects_df) for subjects in predicted_subjects]