# B3 Dividend and Stock Price Collector

A robust system for collecting and analyzing financial data from stocks listed on B3 (Brasil Bolsa Balcão).

## 📊 Description

This project automates the collection of:
- **Dividend history** for all Brazilian stocks
- **Daily price quotes** for all companies listed on B3

The data is organized, analyzed, and exported to easy-to-use CSV files, enabling various financial analyses, especially focused on dividend-based investment strategies.

## 🔍 Features

- **Dividend Collection**: Extracts the complete history of dividends distributed by B3 companies
- **Price Quote Collection**: Obtains historical price series of Brazilian stocks since 2015
- **Statistical Analysis**: Calculates important metrics such as:
  - Average and total dividend values per period
  - Ranking of the best dividend payers by year
  - Identification of companies with most consistent payments
- **Organized Export**: Generates standardized and optimized CSV files

## 📚 Libraries Used

- **yfinance**: Unofficial API to access Yahoo Finance financial data
- **pandas**: Efficient manipulation and analysis of financial data
- **numpy**: Support for advanced mathematical and statistical operations
- **logging**: Organized logging system for monitoring and debugging
- **decimal**: Appropriate precision for financial calculations

## 🌟 Why This Project Is Useful

### For Investors and Traders:
- **Yield Analysis**: Ready-to-use data for calculating dividend yield and other metrics
- **Complete History**: Access to historical data for trend analysis
- **Consistency**: Identification of companies most consistent in dividend payments

### For Analysts and Researchers:
- **Structured Data**: Financial information organized in a format ready for analysis
- **Automation**: Eliminates manual work of collecting data from multiple sources
- **Scalability**: Processes hundreds of stocks simultaneously

## 🚀 How to Use

1. Install dependencies:
```
pip install yfinance pandas numpy
```

2. Run the main script:
```
python dividend_collector.py
```

3. Data will be saved in the `data/` directory:
   - `dividends.csv`: Detailed history of all dividends
   - `dividends_year.csv`: Annual summary of dividends by stock
   - `quotes/quote_detail_XXXX.csv`: Daily quotes by year

## 📌 Important Notes

- The code is optimized for the Brazilian market (B3)
- Uses a predefined list of tickers for greater efficiency
- Implements retry mechanism to ensure data completeness
- Financial value precision is carefully handled

## 🔧 Possible Extensions

- Export to database
- Graphical interface for data visualization
- Automatic calculation of indicators such as P/E, ROE, Dividend Yield
- Alerts for ex-dividend dates
