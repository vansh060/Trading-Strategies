#!/usr/bin/env python
# coding: utf-8

# In[1]:


def mean_reversion_strategy(num_years, num_stocks=5):
    get_ipython().system('pip install plotly')
    import pandas as pd 
    import yfinance as yf 
    from datetime import datetime, timedelta 
    import numpy as np 
    import plotly.graph_objects as go 
    #fetching the dow jones industrial average constituents from wikipedia using pandas 
    url = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
    tables = pd.read_html(url)

    dow_jones_constituents = tables[2]
    
    dow_jones_constituents.to_csv('Downloads/dow_jones_constituents.csv', index = False)
    
    stock_data = pd.DataFrame()
    start_date = datetime.now() - timedelta(days = 365*num_years)
    for symbol in dow_jones_constituents['Symbol']:
        try:
            ticker_data = yf.download(symbol, start = start_date, progress = False)
            ticker_data['Symbol'] = symbol 
            stock_data = pd.concat([stock_data, ticker_data], axis =0)
        except Exception as e:
            print(f'Error occured for symbol: {symbol}, {str(e)}')
            
    stock_data.to_csv('Downloads/DJ_data.csv')
            
    dj_constituents = pd.read_csv('Downloads/dow_jones_constituents.csv')
    dj_data = pd.read_csv('Downloads/DJ_data.csv')

    dj_constituents.fillna(method = 'ffill', inplace = True)
    dj_data.fillna(method = 'ffill', inplace = True)
    dj_data.sort_values(['Symbol', 'Date'], inplace = True)
    dj_data['Daily Return'] = dj_data.groupby('Symbol')['Adj Close'].pct_change()
    dj_data_without_first_day = dj_data[dj_data['Date'] != dj_data['Date'].min()]

    lowest_daily_returns = dj_data_without_first_day.groupby('Date', as_index=False).apply(
        lambda x: x.nsmallest(num_stocks, 'Daily Return')
    ).reset_index(drop=True)
    
    
    lowest_daily_returns['Date'] = pd.to_datetime(lowest_daily_returns['Date'])
    dj_data['Date'] = pd.to_datetime(dj_data['Date'])

    initial_capital = 1000000
    results = pd.DataFrame(columns=['Date', 'Capital'])
    results = results.set_index('Date')


    lowest_daily_returns = lowest_daily_returns.sort_values('Date')
    dj_data = dj_data.sort_values('Date')

        # Getting all unique dates
    unique_dates = lowest_daily_returns['Date'].unique()
    
    for i, current_date in enumerate(unique_dates[:-1]):  # Exclude last date
        next_date = unique_dates[i + 1]

        # Selecting "num_stocks" worst performers for "current_date"
        current_day_losers = lowest_daily_returns[lowest_daily_returns['Date'] == current_date].nsmallest(num_stocks,  'Daily Return')

        if len(current_day_losers) < num_stocks:
            continue

        # Calculating amount invested per stock
        amount_per_stock = initial_capital / num_stocks

        total_value = 0

        for _, stock in current_day_losers.iterrows():
            symbol = stock['Symbol']

            # Finding buy and sell prices for stocks to be traded daily
            buy_price = stock['Adj Close']
            sell_price = dj_data[(dj_data['Date'] == next_date) & (dj_data['Symbol'] == symbol)]['Adj Close'].values

            if len(sell_price) == 0 or np.isnan(sell_price[0]) or np.isnan(buy_price):
                continue

            # Calculating number of shares being traded
            shares = amount_per_stock / buy_price
            value = shares * sell_price[0]

            total_value = total_value + value

        initial_capital = total_value if total_value > 0 else initial_capital
        x = pd.DataFrame([next_date, initial_capital]).T
        x.columns = ['Date', 'Capital']
        x.set_index('Date', inplace=True)
        results = pd.concat([results, x])


        
    final_capital = results['Capital'].iloc[-1]
    total_return = (final_capital - 100000) / 100000 * 100

        # Assuming 252 trading days in a year
    trading_days = 252

    # Calculating daily return
    results['Capital Return'] = results['Capital'].pct_change()

        # Annualized return calculation
    annual_return = (1 + results['Capital Return'].mean())**trading_days - 1

        # Annualized volatility calculation
    annual_volatility = results['Capital Return'].std() * (trading_days**0.5)

        # Sharpe Ratio assuming risk-free rate = 0
    sharpe_ratio = annual_return / annual_volatility

    results['Cumulative Return'] = (1 + results['Capital Return']).cumprod()

    running_max = results['Cumulative Return'].cummax()

        # Calculating drawdown
    drawdown = (results['Cumulative Return'] - running_max) / running_max

        # Finding the maximum drawdown
    max_drawdown = drawdown.min()
    
    
        # Printing the calculated metrics
    print('Initial Capital: $100000')
    print(f'Final Capital: ${str(round(final_capital))}')
    print(f'Annualized Return of Strategy : {(annual_return*100):.2f}%')
    print(f'Annualized Volatility of Strategy : {(annual_volatility*100):.2f}%')
    print(f'Sharpe Ratio of Strategy : {sharpe_ratio:.2f}')
    print(f'Max Drawdown of Strategy : {(max_drawdown*100):.2f}%')

        # Fetching the Dow Jones Industrial Average ETF (benchmark) data from Yahoo Finance
    dia_data = yf.download('DIA', start=start_date, progress=False)

       # Calculating the daily returns for DIA (benchmark)
    dia_data['Daily Return'] = dia_data['Adj Close'].pct_change()

       # Calculating the Sharpe ratio for DIA (benchmark)
    annual_return_dia = (1 + dia_data['Daily Return'].mean())**trading_days - 1
    annual_volatility_dia = dia_data['Daily Return'].std() * np.sqrt(trading_days)
    sharpe_ratio_dia = annual_return_dia / annual_volatility_dia

    print(f'Sharpe Ratio of Dow Jones is {round(sharpe_ratio_dia,2)}')
    
    
         # Comparing Sharpe ratios of strategy and the Dow Jones Industrial Average
    if sharpe_ratio > sharpe_ratio_dia:
        print('The strategy outperformed the Dow Jones.')
    else:
        print('The strategy did not outperform the Dow Jones.')

        # Calculating cumulative returns
    results['Cumulative Return'] = (1 + results['Capital Return']).cumprod()
    dia_data['Cumulative Return'] = (1 + dia_data['Daily Return']).cumprod()

        # Calculating portfolio value
    results['Portfolio Value'] = results['Cumulative Return'] * 100000
    dia_data['Portfolio Value'] = dia_data['Cumulative Return'] * 100000

    
    

        # Initialize figure
    fig = go.Figure()

    # Add the Mean Reversion Strategy trace
    fig.add_trace(go.Scatter(
        x=results.index,
        y=results['Portfolio Value'],
        mode='lines',
        name='Mean Reversion Strategy'
    ))

    # Add the DIA trace
    fig.add_trace(go.Scatter(
        x=dia_data.index,
        y=dia_data['Portfolio Value'],
        mode='lines',
        name='DIA'
    ))

    # Update layout with titles and size specifications
    fig.update_layout(
        title='Growth of $1,000,000 Portfolio Over Time',
        xaxis_title='Date',
        yaxis_title='Portfolio Value ($)',
        legend_title='Strategy',
        width=1000,
        height=600
    )

    # Display the plot
    fig.show()
    


# In[2]:


mean_reversion_strategy(num_years = 15, num_stocks=5)








