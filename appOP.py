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


def show():

	if request.method == 'POST':
		data = pd.read_csv("scored.csv")
		#to_format(data)
		message = request.form['message']
	  data = [message]
	return render_template('index.html',data)
	
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        details = request.form
        if details['form_type'] == 'show':
            return show()
    return render_template('index.html')
    
def home():

    return render_template('index.html')
    
if __name__ == '__main__':
    app.run(host='0.0.0.0')
