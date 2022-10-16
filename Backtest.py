#!/usr/bin/env python
# coding: utf-8
#Backtest de setups
#Criado por: Felipe Muller 
#Versão 0.31


#2022-05-27     0.11 -> Adicionado a coluna Entrada
#2022-10-11     0.20 -> Adicionado os calculos de TradeStart/TradeEnd/Trade
#2022-10-13     0.30 -> Testes iniciais
#2022-10-16     0.31 -> Updated informations on graph
#               0.32 -> Added Retorno == "trades"


# In[3]:

import pandas as pd
import numpy as np
import pandas_datareader as pdr
from datetime import datetime, timedelta
from calendar import monthrange
import yfinance as yf
import ta
import sys
import math
from real_br_money_mask import real_br_money_mask

import plotly.graph_objects as go
from plotly.subplots import make_subplots

if not sys.warnoptions: 
    import warnings
    warnings.simplefilter("ignore")



def Backtest_getdata(ticker, startdate, enddate, timeframe, source="yahoo"):
    
    if (timeframe.lower()=="monthly"):
        delta=24
        interval = '1mo'
        new_stardate = pd.to_datetime(startdate) - np.timedelta64(delta+1, 'M')
    
    elif (timeframe.lower()=="weekly"):
        delta=110
        interval = '1wk'
        new_stardate = pd.to_datetime(startdate) - np.timedelta64(delta+1, 'W')
    
    else: #Aqui fica incluso o daily-week-weekday e daily-weekday
        delta=700
        interval = '1d'
        new_stardate = pd.to_datetime(startdate) - np.timedelta64(delta+1, 'D')
    
    if source == "yahoo":
        dados = yf.Ticker(ticker).history(start=new_stardate,end=enddate,interval='1d',auto_adjust = False)
    else:
        path = path
        
    df = dados.dropna(axis = 0)
    
    df['Date_New']=df.index
    df['Day']=df['Date_New'].dt.day
    df['Day']=pd.to_numeric(df['Day'])
    
    if (timeframe=="monthly"):
        df.index = np.where( df['Day']>1,
                            pd.to_datetime(df.index)+ np.timedelta64(+1, 'D'),
                            df.index)
    else:
        df.index = pd.to_datetime(df.index)
    
    df.index.name='Date'
    
    df.drop(columns=['Date_New', 'Day'], axis=1, inplace=True)
    
    return df

def Backtest(df,TimeFrame,DataInicial,DataFinal,CaixaInicial,Lote,CustoCorretora,CustoB3,Impostos,
             Filtro0,Filtro1,Filtro2,Filtro3,Filtro4,
             TradeSystem, Setup,EntradaPriceType,EntradaPriceValue,EntradaPriceCandles,EntradaPriceDiffType,EntradaPriceDiffValue,
             EntradaCandlesStandby
             , TakeProfitType,TakeProfitValueCandle,TakeProfitValue,TakeProfitPriceDiffType,TakeProfitPriceDiffValue
             , StopLossType,StopLossValueCandle,StopLossValue,StopLossPriceDiffType,StopLossPriceDiffValue
             ,Tempo,Retorno) :
    
    def round_to_lower(number, multiple):
        return multiple * math.floor(number / multiple)
    
    #Criação do DF Semanal e Mensasl
    df_d=[]
    df_w=[]
    df_m=[]
    df_d=df.copy()
    agg_dict = {
              'Open': 'first',
              'High': 'max',
              'Low': 'min',
              'Close': 'last',
              'Adj Close': 'last',
              'Dividends': 'sum',
              'Volume': 'sum'}
    df_w = df_d.resample('W').agg(agg_dict)
    df_w["Date_Start"]=df_w.index-timedelta(6)     #Transforma em Weekly
    df_w=df_w.set_index('Date_Start')
    df_m = df_d.resample('MS').agg(agg_dict)       #Transforma em Monthly
    
    #Filtros
    if (TimeFrame.lower()=="daily"):
        df_ctf=df_d.copy()
        df_tfa=df_w.copy()
    elif (TimeFrame.lower()=="weekly"):
        df_ctf=df_w.copy()
        df_tfa=df_m.copy()
    elif (TimeFrame.lower()=="monthly"):
        df_ctf=df_m.copy()
        df_tfa=df_m.copy()
        
    #TradeSetup
    df_ctf['Setup']=1
        
    #Calculo da Entrada
    df_ctf['Entrada']=0.0
    for i in range(df_ctf.shape[0]):
        if i<EntradaPriceCandles:
            df_ctf['Entrada'][i]=float("nan")
        else:
            if (EntradaPriceType.lower()=="highest"):
                df_ctf['Entrada'][i]=df_ctf[EntradaPriceValue].iloc[i-EntradaPriceCandles+1:i+1].max()
            elif (EntradaPriceType.lower()=="lowest"):
                df_ctf['Entrada'][i]=df_ctf[EntradaPriceValue].iloc[i-EntradaPriceCandles+1:i+1].min()
                
    if EntradaPriceType.lower()=="sma":
        df_ctf['Entrada']=ta.trend.SMAIndicator(df_ctf[EntradaPriceValue],window=int(EntradaPriceCandles)).sma_indicator()
    
    if EntradaPriceDiffType.lower()=="f":
        df_ctf["Entrada"]=round(df_ctf["Entrada"]+EntradaPriceDiffValue,2)
    elif EntradaPriceDiffType.lower()=="p":
        df_ctf["Entrada"]=round((1+EntradaPriceDiffValue/100)*df_ctf["Entrada"],2)
        
    df_ctf["Entrada"]=df_ctf["Entrada"].shift(1)  

    #Calculo TakeProfit
    df_ctf['TakeProfit']=0.0
    for i in range(df_ctf.shape[0]):
        if i<TakeProfitValue:
            df_ctf['TakeProfit'][i]=float("nan")
        else:
            if (TakeProfitType.lower()=="highest"):
                df_ctf['TakeProfit'][i]=df_ctf[TakeProfitValueCandle].iloc[i-TakeProfitValue:i+1].max()
            elif (TakeProfitType.lower()=="lowest"):
                df_ctf['TakeProfit'][i]=df_ctf[TakeProfitValueCandle].iloc[i-TakeProfitValue:i+1].min()
                
    if TakeProfitType.lower()=="sma":
        df_ctf['TakeProfit']=ta.trend.SMAIndicator(df_ctf[TakeProfitValueCandle],window=int(TakeProfitValue)).sma_indicator()
    
    if TakeProfitPriceDiffType.lower()=="f":
        df_ctf["TakeProfit"]=round(df_ctf["TakeProfit"]+TakeProfitPriceDiffValue,2)
    elif TakeProfitPriceDiffType.lower()=="p":
        df_ctf["TakeProfit"]=round((1+TakeProfitPriceDiffValue/100)*df_ctf["TakeProfit"],2)
    
    df_ctf["TakeProfit"]=df_ctf["TakeProfit"].shift(1)
    
    #Calculo StopLoss
    df_ctf['StopLoss']=0.0
    for i in range(df_ctf.shape[0]):
        try:
            if i<StopLossValue:
                df_ctf['StopLoss'][i]=0
            else:
                if (StopLossType.lower()=="highest"):
                    df_ctf['StopLoss'][i]=df_ctf[StopLossValueCandle].iloc[i-StopLossValue:i+1].max()
                elif (StopLossType.lower()=="lowest"):
                    df_ctf['StopLoss'][i]=df_ctf[StopLossValueCandle].iloc[i-StopLossValue:i+1].min()
        except:
            df_ctf['StopLoss'][i]=0
            
    if StopLossType.lower()=="sma":
        df_ctf['StopLoss']=ta.trend.SMAIndicator(df_ctf[StopLossValueCandle],window=int(StopLossValue)).sma_indicator()
    
    if StopLossPriceDiffType.lower()=="f":
        df_ctf["StopLoss"]=round(df_ctf["StopLoss"]+StopLossPriceDiffValue,2)
    elif StopLossPriceDiffType.lower()=="p":
        df_ctf["StopLoss"]=round((1+StopLossPriceDiffValue/100)*df_ctf["StopLoss"],2)
    
    df_ctf["StopLoss"]=df_ctf["StopLoss"].shift(1)
    
    
    
    #Cria o DF Filtros e coloca os valores e checa as condicoes tanto no DF_CurrentTimeFrame quanto no DF_TimeFrameAbove
    Filtros = [[Filtro0], [Filtro1], [Filtro2], [Filtro3], [Filtro4]]
    Filtros = pd.DataFrame(Filtros, columns = ['Filtro'])
    df_ctf['Filtro0Cond']=True
    df_ctf['Filtro1Cond']=True
    df_ctf['Filtro2Cond']=True
    df_ctf['Filtro3Cond']=True
    df_ctf['Filtro4Cond']=True
    
    for f in range(5):
        if (Filtros["Filtro"][f]!=""): 
            FiltroSplit=Filtros["Filtro"][f].split(";")
            df_ctf["Filtro"+str(f)]=""
            df_ctf["Filtro"+str(f)+"Cond"]="True"
            df_tfa["Filtro"+str(f)]=""
            df_tfa["Filtro"+str(f)+"Cond"]="True"
            if (FiltroSplit[2]=="CTF"):
                if (FiltroSplit[0]=="SMA") | (FiltroSplit[0]=="EMA"): 
                    if (FiltroSplit[0]=="SMA"):
                        df_ctf["Filtro"+str(f)]=ta.trend.SMAIndicator(df_ctf["Close"],window=int(FiltroSplit[1])).sma_indicator()
                    elif(FiltroSplit[0]=="EMA"):
                        df_ctf["Filtro"+str(f)]=ta.trend.EMAIndicator(df_ctf["Close"],window=int(FiltroSplit[1])).ema_indicator()
                    if (FiltroSplit[3]=="ASC"):
                        df_ctf["Filtro"+str(f)+"Cond"]=np.where(
                                                        df_ctf["Filtro"+str(f)]>=df_ctf["Filtro"+str(f)].shift(),
                                                        True,
                                                        False
                                                        )
                    elif (FiltroSplit[3]=="ABV"):
                        df_ctf["Filtro"+str(f)+"Cond"]=np.where(
                                                        df_ctf["Close"]>=df_ctf["Filtro"+str(f)],
                                                        True,
                                                        False
                                                        )
                    elif (FiltroSplit[3]=="ASC&ABV"):
                        df_ctf["Filtro"+str(f)+"Cond"]=np.where(
                                                        ((df_ctf["Filtro"+str(f)]>=df_ctf["Filtro"+str(f)].shift())) 
                                                        & (df_ctf["Close"]>=df_ctf["Filtro"+str(f)]),
                                                        True,
                                                        False
                                                        ) 
                
                
                if (FiltroSplit[0]=="IFR"): 
                    df_ctf["Filtro"+str(f)]=ta.momentum.RSIIndicator(df_ctf["Close"],window=int(FiltroSplit[1])).rsi()
                    if (FiltroSplit[3]=="ASC"):
                        df_ctf["Filtro"+str(f)+"Cond"]=np.where(
                                                        df_ctf["Filtro"+str(f)]>=df_ctf["Filtro"+str(f)].shift(),
                                                        True,
                                                        False
                                                        )
                    elif (FiltroSplit[3]=="BTW"):
                        FiltroValue=FiltroSplit[4].split(",")
                        df_ctf["Filtro"+str(f)+"Cond"]=np.where(
                                                        ((df_ctf["Filtro"+str(f)]>=float(FiltroValue[0])) 
                                                     &  (df_ctf["Filtro"+str(f)]<=float(FiltroValue[1]))) 
                                                        , True
                                                        , False
                                                        )
                    elif (FiltroSplit[3]=="ASC&BTW"):
                        FiltroValue=FiltroSplit[4].split(",")
                        df_ctf["Filtro"+str(f)+"Cond"]=np.where(
                                                        ((df_ctf["Filtro"+str(f)]>=df_ctf["Filtro"+str(f)].shift())
                                                     &   (df_ctf["Filtro"+str(f)]>=float(FiltroValue[0])) 
                                                     &  (df_ctf["Filtro"+str(f)]<=float(FiltroValue[1]))) 
                                                        , True
                                                        , False
                                                        )                        
            elif (FiltroSplit[2]=="TFA"):
                if (FiltroSplit[0]=="SMA") | (FiltroSplit[0]=="EMA"): 
                    if (FiltroSplit[0]=="SMA"):
                        df_tfa["Filtro"+str(f)]=ta.trend.SMAIndicator(df_tfa["Close"],window=int(FiltroSplit[1])).sma_indicator()
                    elif(FiltroSplit[0]=="EMA"):
                        df_tfa["Filtro"+str(f)]=ta.trend.EMAIndicator(df_tfa["Close"],window=int(FiltroSplit[1])).ema_indicator()
                    if (FiltroSplit[3]=="ASC"):
                        df_tfa["Filtro"+str(f)+"Cond"]=np.where(
                                                        df_tfa["Filtro"+str(f)]>=df_tfa["Filtro"+str(f)].shift(),
                                                        True,
                                                        False
                                                        )
                    elif (FiltroSplit[3]=="ABV"):
                        df_tfa["Filtro"+str(f)+"Cond"]=np.where(
                                                        df_tfa["Close"]>=df_tfa["Filtro"+str(f)],
                                                        True,
                                                        False
                                                        )
                    elif (FiltroSplit[3]=="ASC&ABV"):
                        df_tfa["Filtro"+str(f)+"Cond"]=np.where(
                                                        ((df_tfa["Filtro"+str(f)]>=df_tfa["Filtro"+str(f)].shift())) 
                                                        & (df_tfa["Close"]>=df_tfa["Filtro"+str(f)]),
                                                        True,
                                                        False
                                                        ) 
                
                
                #-------------IFR------------------------------
                if (FiltroSplit[0]=="IFR"): 
                    df_tfa["Filtro"+str(f)]=ta.momentum.RSIIndicator(df_tfa["Close"],window=int(FiltroSplit[1])).rsi()
                    if (FiltroSplit[3]=="ASC"):
                        df_tfa["Filtro"+str(f)+"Cond"]=np.where(
                                                        df_tfa["Filtro"+str(f)]>=df_tfa["Filtro"+str(f)].shift(),
                                                        True,
                                                        False
                                                        )
                    elif (FiltroSplit[3]=="BTW"):
                        FiltroValue=FiltroSplit[4].split(",")
                        df_tfa["Filtro"+str(f)+"Cond"]=np.where(
                                                        ((df_tfa["Filtro"+str(f)]>=float(FiltroValue[0])) 
                                                     &  (df_tfa["Filtro"+str(f)]<=float(FiltroValue[1]))) 
                                                        , True
                                                        , False
                                                        )
                    elif (FiltroSplit[3]=="ASC&BTW"):
                        FiltroValue=FiltroSplit[4].split(",")
                        df_tfa["Filtro"+str(f)+"Cond"]=np.where(
                                                        ((df_tfa["Filtro"+str(f)]>=df_tfa["Filtro"+str(f)].shift())
                                                     &   (df_tfa["Filtro"+str(f)]>=float(FiltroValue[0])) 
                                                     &  (df_tfa["Filtro"+str(f)]<=float(FiltroValue[1]))) 
                                                        , True
                                                        , False
                                                    )  
    df_ctf.dropna(axis = 0 , inplace=True)
    
    if (TradeSystem.lower()=="buy") or (TradeSystem.lower()=="b") or (TradeSystem.lower()=="legadofinanceiro") or (TradeSystem.lower()=="lf"): 
        if (EntradaPriceType.lower()!="close"):
            df_ctf['Setup']=df_ctf['Setup'].shift(1)
            df_ctf['Filtro0Cond']=df_ctf['Filtro0Cond'].shift(1)
            df_ctf['Filtro1Cond']=df_ctf['Filtro1Cond'].shift(1)
            df_ctf['Filtro2Cond']=df_ctf['Filtro2Cond'].shift(1)
            df_ctf['Filtro3Cond']=df_ctf['Filtro3Cond'].shift(1)
            df_ctf['Filtro4Cond']=df_ctf['Filtro4Cond'].shift(1)
            
            df_ctf['TradeStart']=np.where((df_ctf.index>=DataInicial)
                                          & (df_ctf['Setup']==1)
                                          & (df_ctf['High']>=df_ctf['Entrada'])
                                          & (df_ctf['Low']<=df_ctf['Entrada'])
                                          & (df_ctf['Filtro0Cond']==True)
                                          & (df_ctf['Filtro1Cond']==True)
                                          & (df_ctf['Filtro2Cond']==True)
                                          & (df_ctf['Filtro3Cond']==True)
                                          & (df_ctf['Filtro4Cond']==True)
                                    ,1000
                                    ,0)

            df_ctf['TradeEnd']=np.where((df_ctf['High']>=df_ctf['StopLoss'])
                                          & (df_ctf['Low']<=df_ctf['StopLoss'])
                                       ,1001
                                       ,0)
            df_ctf['TradeEnd']=np.where((df_ctf['High']>=df_ctf['TakeProfit'])
                                          & (df_ctf['Low']<=df_ctf['TakeProfit'])
                                       ,1002
                                       ,0)
            position = df_ctf.index.get_loc(DataInicial,method='pad')
                
            df_ctf=df_ctf.iloc[position-1:]
            
            df_ctf=df_ctf.reset_index()
            df_ctf['Trade']=""
            df_ctf['TradeCount']=0
            df_ctf['Valor']=0.
            df_ctf['Corretagem']=0.
            df_ctf['Perc']=0.
            df_ctf['PercAcum']=0.
            df_ctf['Drawdown']=0.
            df_ctf['Caixa']=CaixaInicial
            df_ctf['LucroAcoes']=0.
            df_ctf['LucroAcoesAcum']=0.
            df_ctf['LucroDividendos']=0.
            df_ctf['LucroDividendosAcum']=0.
            
            for i in range(df_ctf.shape[0]):
                try:
                    if (df_ctf['Trade'][i-1]!="Hold"):
                        if (df_ctf['TakeProfit'][i]>df_ctf['Entrada'][i]):
                            if (df_ctf['TradeStart'][i]==1000) and (df_ctf['TradeEnd'][i]==1001):
                                df_ctf['Trade'][i]="BuySellSL"
                                df_ctf['TradeCount'][i]=1
                            elif (df_ctf['TradeStart'][i]==1000) and (df_ctf['TradeEnd'][i]==1002):
                                df_ctf['Trade'][i]="BuySellTP"
                                df_ctf['TradeCount'][i]=1
                            elif (df_ctf['TradeStart'][i]==1000) and (Tempo==1):
                                df_ctf['Trade'][i]="BuySellClose"
                                df_ctf['TradeCount'][i]=1
                            elif (df_ctf['TradeStart'][i]==1000) and (df_ctf['TradeEnd'][i]==0):
                                df_ctf['Trade'][i]="Buy"
                                df_ctf['TradeCount'][i]=1
                    if (df_ctf['Trade'][i-1]=="Buy") or (df_ctf['Trade'][i-1]=="Hold"):
                        if (df_ctf['TradeEnd'][i]==1001):
                            df_ctf['Trade'][i]="SellSL"
                            df_ctf['TradeCount'][i]=df_ctf['TradeCount'][i-1]+1
                        elif (df_ctf['TradeEnd'][i]==1002):
                            df_ctf['Trade'][i]="SellTP"
                            df_ctf['TradeCount'][i]=df_ctf['TradeCount'][i-1]+1
                        elif (df_ctf['TradeCount'][i-1]==Tempo-1):
                            df_ctf['Trade'][i]="SellClose"
                            df_ctf['TradeCount'][i]=Tempo
                        else:
                            df_ctf['Trade'][i]="Hold"
                            df_ctf['TradeCount'][i]=df_ctf['TradeCount'][i-1]+1

                        df_ctf['Entrada'][i]=df_ctf['Entrada'][i-1]

                    Qty=0
                    Valor=0.
                    Corretagem=0.
                    Perc=0.
                    PercAcum=0.
                    Drawdown=0.
                    LucroAcoes=0.
                    ValorFinal=0.
                    PercFinal=0.
                    
                    if (TradeSystem.lower()=="buy") or (TradeSystem.lower()=="b")  or (TradeSystem.lower()=="legadofinanceiro")  or (TradeSystem.lower()=="lf"):
                            
                        if (df_ctf['Trade'][i]=="SellTP") or (df_ctf['Trade'][i]=="BuySellTP"):
                            Qty=round_to_lower(df_ctf['Caixa'][i-1]/df_ctf['Entrada'][i], Lote)
                            Perc=(df_ctf['TakeProfit'][i]-df_ctf['Entrada'][i])/df_ctf['Entrada'][i]
                            Valor=Perc*Qty*df_ctf['Entrada'][i]
                            Corretagem=(2*CustoCorretora)*(1+Impostos/100)+(Qty*df_ctf['Entrada'][i])*(CustoB3/100)+(Qty*df_ctf['TakeProfit'][i])*(CustoB3/100)
                            ValorFinal=(Valor-Corretagem)
                            PercFinal=ValorFinal/(df_ctf['Entrada'][i]*Qty)
                        elif (df_ctf['Trade'][i]=="SellSL") or (df_ctf['Trade'][i]=="BuySellSL"):
                            Qty=round_to_lower(df_ctf['Caixa'][i-1]/df_ctf['Entrada'][i], Lote)
                            Perc=(df_ctf['StopLoss'][i]-df_ctf['Entrada'][i])/df_ctf['Entrada'][i]
                            Valor=Perc*Qty*df_ctf['Entrada'][i]
                            Corretagem=(2*CustoCorretora)*(1+Impostos/100)+(Qty*df_ctf['Entrada'][i])*(CustoB3/100)+(Qty*df_ctf['TakeProfit'][i])*(CustoB3/100)
                            ValorFinal=(Valor-Corretagem)
                            PercFinal=ValorFinal/(df_ctf['Entrada'][i]*Qty)
                            
                        elif (df_ctf['Trade'][i]=="SellClose") or (df_ctf['Trade'][i]=="BuySellClose"):
                            Qty=round_to_lower(df_ctf['Caixa'][i-1]/df_ctf['Entrada'][i], Lote)
                            Perc=(df_ctf['Close'][i]-df_ctf['Entrada'][i])/df_ctf['Entrada'][i]
                            Valor=Perc*Qty*df_ctf['Entrada'][i]
                            Corretagem=(2*CustoCorretora)*(1+Impostos/100)+(Qty*df_ctf['Entrada'][i])*(CustoB3/100)+(Qty*df_ctf['TakeProfit'][i])*(CustoB3/100)
                            ValorFinal=(Valor-Corretagem)
                            PercFinal=ValorFinal/(df_ctf['Entrada'][i]*Qty)
                            
                        df_ctf['Valor'][i]=round(Valor,2)
                        df_ctf['Corretagem'][i]=round(Corretagem,2)
                        df_ctf['Perc'][i]=round(PercFinal,5)
                        df_ctf['PercAcum'][i]=df_ctf['Perc'][i]+df_ctf['PercAcum'][i-1]
                        df_ctf['Drawdown'][i]=df_ctf['Drawdown'][i-1]+df_ctf['Perc'][i]
                        if (df_ctf['Drawdown'][i]>0):
                            df_ctf['Drawdown'][i]=0
                        
                        

                        if (TradeSystem.lower()=="buy") or (TradeSystem.lower()=="b"):
                            df_ctf['Caixa'][i]=df_ctf['Caixa'][i-1]+df_ctf['Valor'][i]-df_ctf['Corretagem'][i]

                        elif (TradeSystem.lower()=="legadofinanceiro") or (TradeSystem.lower()=="lf"):
                            df_ctf['Caixa'][i]=df_ctf['Caixa'][i-1]+df_ctf['Valor'][i]-df_ctf['Corretagem'][i]
                            if (df_ctf['Caixa'][i]>CaixaInicial):
                                df_ctf['LucroAcoes'][i]=round_to_lower((df_ctf['Caixa'][i]-CaixaInicial)/df_ctf['Close'][i],1)
                                df_ctf['Caixa'][i]=df_ctf['Caixa'][i]-round(df_ctf['LucroAcoes'][i]*df_ctf['Close'][i],2)
                            df_ctf['LucroAcoesAcum'][i]=df_ctf['LucroAcoesAcum'][i-1]+df_ctf['LucroAcoes'][i]
                            df_ctf['LucroDividendos'][i]=round(df_ctf['LucroAcoesAcum'][i-1]*df_ctf['Dividends'][i],2)
                            df_ctf['LucroDividendosAcum'][i]=df_ctf['LucroDividendosAcum'][i-1]+df_ctf['LucroDividendos'][i]
                            
                except:
                    df_ctf['Trade']=""
                    df_ctf['TradeCount']=0
 
    try:
        df_ctf.set_index('Date_Start', inplace=True)
    except:
        df_ctf.set_index('Date', inplace=True)
    df=df_ctf.loc[DataInicial:DataFinal]
    
    if Retorno.lower()=="table":
        return df
    
    elif (Retorno.lower()=="trades"):
        pattern = ['Buy','Sell']
        trades = df.loc[df.Trade.str.contains('|'.join(pattern)),:]
        
        return trades
    
    elif (Retorno.lower() == 'summary') or (Retorno.lower() == 'row'):
            Entrada=EntradaPriceType+';'+EntradaPriceValue+';'+str(EntradaPriceCandles)+';'+EntradaPriceDiffType.upper()+';'+str(EntradaPriceDiffValue)
            #EntradaCandlesStandby
            TakeProfit = TakeProfitType+';'+TakeProfitValueCandle+';'+str(TakeProfitValue)+';'+TakeProfitPriceDiffType.upper()+';'+str(TakeProfitPriceDiffValue)
            if (StopLossType!=''):
                StopLoss = StopLossType+';'+StopLossValueCandle+';'+str(StopLossValue)+';'+StopLossPriceDiffType.upper()+';'+str(StopLossPriceDiffValue)
            else:
                StopLoss = ''
            StopTempo = Tempo
            DataInicial = df.index[0]
            DataFinal = df.index[-1]
            GainValor = df[df['Valor']>0].sum()['Valor']
            LossValor = df[df['Valor']<0].sum()['Valor']
            Custos = df['Corretagem'].sum()
            ResulLiquido = GainValor+LossValor-Custos
            ResulLiquidoPerc=df['Perc'].sum()
            TotalTrades = df[df['TradeCount']==1].sum()['TradeCount']
            WinRate= df[df['Valor']>0].count()['Trade']/TotalTrades
            MediaGain = df[df['Valor']>0].mean()['Valor']
            MediaLoss = df[df['Valor']<0].mean()['Valor']
            MediaGainPerc = df[df['Valor']>0].mean()['Perc']
            MediaLossPerc = df[df['Valor']<0].mean()['Perc']
            Drawdown = df['Drawdown'].min()
            MediaCandleTrade = df[df['TradeCount']>0].count()['TradeCount']/TotalTrades
            MediaEmTrade = df[df['TradeCount']>0].count()['TradeCount']/df.shape[0]
            CaixaAtual = df['Caixa'][-1]
            LucroAcoes = df['LucroAcoesAcum'][-1]
            ExpMat = ((1+MediaGain/abs(MediaLoss))*WinRate)-1
            ProFactor = abs(GainValor/LossValor)
            
            
            if (Retorno.lower()=="summary"):
                data = {'Valor': [TradeSystem
                                    , TimeFrame.capitalize()
                                    , Entrada
                                    , Filtro0
                                    , Filtro1
                                    , Filtro2
                                    , Filtro3
                                    , Filtro4
                                    , TakeProfit    
                                    , StopLoss
                                    , StopTempo
                                    , pd.to_datetime(DataInicial).strftime('%d/%m/%Y')
                                    , pd.to_datetime(DataFinal).strftime('%d/%m/%Y')
                                    , 'R$ '+real_br_money_mask(GainValor)
                                    , 'R$ '+real_br_money_mask(LossValor) 
                                    , 'R$ '+real_br_money_mask(Custos)
                                    , 'R$ '+real_br_money_mask(ResulLiquido)
                                    , TotalTrades
                                    , str(round(WinRate*100,2))+" %"
                                    , 'R$ '+real_br_money_mask(MediaGain)  
                                    , 'R$ '+real_br_money_mask(MediaLoss)
                                    , ''
                                    , round(MediaCandleTrade,2)
                                    , str(round(MediaEmTrade*100,2))+" %"
                                    , 'R$ '+real_br_money_mask(CaixaAtual)
                                    , LucroAcoes  
                                    , round(ExpMat,2)
                                    , round(ProFactor,2)
                                    ],
                        'Porcentagem': [''
                                    , ''    
                                    , ''
                                    , ''
                                    , ''
                                    , ''    
                                    , ''
                                    , ''
                                    , ''
                                    , ''
                                    , ''    
                                    , ''
                                    , ''
                                    , ''
                                    , ''  
                                    , ''
                                    , str(round(ResulLiquidoPerc*100,2))+" %"
                                    , ''
                                    , ''
                                    , str(round(MediaGainPerc*100,2))+" %"  
                                    , str(round(MediaLossPerc*100,2))+" %"
                                    , str(round(Drawdown*100,2))+" %"
                                    , ''
                                    , ''
                                    , ''
                                    , ''  
                                    , ''
                                    , ''    
                                    ],
                        'Outras Infos': [''
                                    , ''    
                                    , ''
                                    , ''
                                    , ''
                                    , ''    
                                    , ''
                                    , ''
                                    , ''
                                    , ''
                                    , ''    
                                    , ''
                                    , ''
                                    , ''
                                    , ''  
                                    , ''
                                    , ''
                                    , ''
                                    , ''
                                    
                                    ,''
                                    , '' 
                                    , ''
                                    , ''
                                    , ''
                                    , ''
                                    , 'R$ '+real_br_money_mask(LucroAcoes*df['Close'][-1])+' + Dividendos '+real_br_money_mask(df_ctf['LucroDividendosAcum'][-1]) 
                                    , ''
                                    , ''    
                                    ]
                }

                summary = pd.DataFrame(data, index=['Trade System'
                                                    , 'Timeframe'
                                                    , 'Entrada'
                                                    , 'Filtro0'
                                                    , 'Filtro1'
                                                    , 'Filtro2'
                                                    , 'Filtro3'
                                                    , 'Filtro4'
                                                    , 'TakeProfit'
                                                    , 'StopLoss'
                                                    , 'StopTempo'
                                                    , 'Data Inicial'
                                                    , 'Data Final'
                                                    , 'Gain'
                                                    , 'Loss'
                                                    , 'Custos'
                                                    , 'Resultado Líquido'
                                                    , 'Quantidade de Trades'
                                                    , 'WinRate'
                                                    , 'Média Gain'
                                                    , 'Media Loss'
                                                    , 'Max Drawdown'
                                                    , 'Média Candle por Trade'
                                                    , 'Tempo em Trade'
                                                    , 'Caixa Atual'
                                                    , 'Se Lucro em Ações'
                                                    , 'Expectativa Matemática'
                                                    , 'Profit Factor'
                                                    ])

                return summary
            elif Retorno.lower()=="row":
                row = pd.DataFrame({'Entrada':[Entrada]
                                   , 'Filtro 0':[Filtro0]
                                   , 'Filtro 1':[Filtro1]
                                   , 'Filtro 2':[Filtro2]
                                   , 'Filtro 3':[Filtro3]
                                   , 'Filtro 4':[Filtro4]      
                                   , 'TakeProfit':[TakeProfit]
                                   , 'StopLoss':[StopLoss]
                                   , 'StopTempo':[StopTempo]
                                   , 'DataInicial':[DataInicial]
                                   , 'DataFinal':[DataFinal]
                                   , 'GainValor':[GainValor]
                                   , 'LossValor':[LossValor]
                                   , 'Custos':[Custos]
                                   , 'ResulLiquido':[ResulLiquido]
                                   , 'ResulLiquidoPerc':[ResulLiquidoPerc]
                                   , 'TotalTrades':[TotalTrades]
                                   , 'WinRate':[WinRate]
                                   , 'MediaCandleTrade':[MediaCandleTrade]
                                   , 'MediaEmTrade':[MediaEmTrade]
                                   , 'Drawdown':[Drawdown] 
                                   , 'CaixaAtual':[CaixaAtual]
                                   , 'LucroAcoes':[LucroAcoes]
                                   , 'ExpMat':[ExpMat]
                                   , 'ProFactor':[ProFactor]
                                  })
                return row
    elif Retorno.lower()=="graph":
        
        pattern = ['Buy','Sell']
        annotations = df.loc[df.Trade.str.contains('|'.join(pattern)),:]
        annot = annotations[['Trade','Entrada','TakeProfit','Close']]
        annot.reset_index(inplace=True)
        try:
            annot = annot.rename(columns={'Date_Start': 'Date'})
        except:
            annot=annot.copy()

        fig = make_subplots(rows=4, cols=1
                            , shared_xaxes=True
                            ,vertical_spacing=0.01
                            , row_width=[0.22, 0.2, 0.20, 0.4])

        fig.append_trace(go.Candlestick(x=df.index
                    , open=df['Open']
                    , high=df['High']
                    , low=df['Low']
                    , close=df['Close']
                    , name = "Candlesticks"
                    , increasing_line_color= 'black', decreasing_line_color= 'black'
                    , increasing_line_width= 1, decreasing_line_width= 1
                    , increasing_fillcolor= 'white', decreasing_fillcolor= 'black'
                    , row = 1 , col = 1,
                    ))

        fig.append_trace(go.Scatter(x=df.index, y=df.TakeProfit, name = 'TakeProfit', line=dict(color='orange', width=1))
                         , row=1, col=1)

        fig.append_trace(go.Scatter(
                x = df.index,
                y = df['PercAcum']*100,
                name = 'Percentual Acumulado(%)' 
            ), row=2, col=1)              

        if (TradeSystem.lower()=="legadofinanceiro") or (TradeSystem.lower()=="lf"): 
            fig.append_trace(go.Scatter(
                    x=df.index,
                    y=df['LucroAcoesAcum'],
                    name = 'Lucros em Ações Acumulado'
                ), row=3, col=1)
        else:
            fig.append_trace(go.Scatter(
                    x=df.index,
                    y=df['Caixa'],
                ), row=3, col=1)

        fig.append_trace(go.Scatter(
            x = df.index,
            y = df['Drawdown']*100,
            name = 'Drawdown(%)' 
        ), row=4, col=1)              

        #Buy
        i=0

        for i in range(annot.shape[0]):
            if (annot['Trade'][i]=="Buy"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['Entrada'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="green"
                    , ax=0
                    , ay=30
                    )
            elif (annot['Trade'][i]=="SellTP"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['TakeProfit'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="red"
                    , ax=0
                    , ay=-20
                    )
            elif (annot['Trade'][i]=="SellClose"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['Close'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="red"
                    , ax=0
                    , ay=-20
                )
            elif (annot['Trade'][i]=="SellSL"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['StopLoss'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="red"
                    , ax=0
                    , ay=-20
                    )
            elif (annot['Trade'][i]=="BuySellTP"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['Entrada'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="green"
                    , ax=0
                    , ay=30
                    )

                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['TakeProfit'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="red"
                    , ax=0
                    , ay=-20
                    )

            elif (annot['Trade'][i]=="BuySellSL"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['Entrada'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="green"
                    , ax=0
                    , ay=30
                    )

                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['StopLoss'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="red"
                    , ax=0
                    , ay=-20
                    )
            elif (annot['Trade'][i]=="BuySellClose"):
                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['Entrada'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="green"
                    , ax=0
                    , ay=30
                    )

                fig.add_annotation(
                    x=annot['Date'][i]
                    , y=annot['Close'][i]
                    , yanchor='bottom'
                    , showarrow=True
                    , arrowhead=1
                    , arrowsize=1
                    , arrowwidth=2
                    , arrowcolor="red"
                    , ax=0
                    , ay=-20
                    )



        fig.update(layout_xaxis_rangeslider_visible=False)
        fig.update_layout(height=1000, width=800)
        
        return fig.show()
