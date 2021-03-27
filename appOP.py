from flask import Flask,render_template,url_for,request
import pandas as pd
import numpy as np
from nltk.stem.porter import PorterStemmer
import re
import string
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from Scoring import to_format

## Applications of OP

#mettre les defs ici


app = Flask(__name__)
data = pd.read_csv("scored.csv")
to_format(data)

@app.route('/')
def home():
    return render_template('index.html')
if __name__ == '__main__':
    app.run(host='0.0.0.0')
