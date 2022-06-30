### Correlating Twitter Sentiment to Bitcoin Market Behaviour
###### This project intends to find correlations between average sentiment per time interval and BTC market behaviour per corresponding interval
#### Tech Stack
* REST API
* PostgreSQL
* VADER (Valence Aware Dictionary sEntiment Reasoner)
* NLTK
* Scikit-Learn
* Plotly
* Flask
###### Process:
1. Data Management:
   1. Extract tweets from Twitter target account per given interval => store it in raw tweets db
   2. Preprocess extracted tweets => store it in preprocessed tweets db
   3. Extract BTC stats hourly, daily => store it in db
2. Dashboard:
   1. Hourly/Daily analysis
   2. Display automatically updated graphs

#### Future Intent
1. Gather sufficient amount of data
2. Perform time series analysis using gathered data