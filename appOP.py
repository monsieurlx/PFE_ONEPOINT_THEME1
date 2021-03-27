import re
from flask import Flask,render_template,url_for,request
import pandas as pd
import numpy as np
from nltk.stem.porter import PorterStemmer

import string
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from Scoring import to_format
## Applications of OP

#mettre les defs ici


app = Flask(__name__)


data = pd.read_csv("scored.csv",sep = '\t')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/show',methods=['POST'])
def show():
    if request.method == 'POST':
      data = pd.read_csv("scored.csv")
    return render_template('index.html', data.to_html) #ou créer autre page


if __name__ == '__main__':
    app.run(host='0.0.0.0')
