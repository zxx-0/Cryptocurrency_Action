import ccxt
import pandas as pd
import pandas_ta as ta
from datetime import datetime
import pytz

# 创建OKEx交易所实例
exchange = ccxt.okx()

def fetch_market_data(symbol, timeframe, limit=100):
    """
    获取指定交易对的K线数据，并计算技术指标。
    增强了数据验证和错误处理。
    """
    try:
        # 获取更多数据点以确保计算的准确性
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit + 50)
        if not ohlcv or len(ohlcv) < limit:
            print(f"警告：{symbol} 在 {timeframe} 时间框架中的数据不足")
            return None
            
        # 创建DataFrame
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # 验证数据时间戳是否最新
        current_time = exchange.milliseconds()
        last_candle_time = df['timestamp'].iloc[-1]
        max_delay = {
            '15m': 15 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000
        }
        
        if (current_time - last_candle_time) > max_delay[timeframe]:
            print(f"警告: {symbol} {timeframe} 数据不是最新的")
            return None

        # 数据类型转换和验证
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if df[col].isnull().any():
                print(f"警告：{symbol} 在 {timeframe} 时间框架中存在无效的{col}数据")
                return None

        # 计算技术指标
        try:
            # MACD
            macd = ta.macd(df['close'])
            macd.columns = ['MACD', 'MACD_signal', 'MACD_hist']
            df = pd.concat([df, macd], axis=1)

            # RSI
            df['RSI_7'] = ta.rsi(df['close'], length=7)

            # 移动平均线
            if timeframe in ['1h', '15m']:
                df['MA5'] = ta.sma(df['close'], length=5)
                df['MA10'] = ta.sma(df['close'], length=10)
                df['MA5_prev'] = df['MA5'].shift(1)
                df['MA10_prev'] = df['MA10'].shift(1)
            else:  # 4h和1d
                df['MA5'] = ta.sma(df['close'], length=5)
                df['MA20'] = ta.sma(df['close'], length=20)
                df['MA5_prev'] = df['MA5'].shift(1)
                df['MA20_prev'] = df['MA20'].shift(1)

            if timeframe == '1d':
                df['MA50'] = ta.sma(df['close'], length=50)

        except Exception as e:
            print(f"计算技术指标时出错 {symbol} {timeframe}: {str(e)}")
            return None

        # 验证关键指标是否存在
        required_columns = {
            '1d': ['close', 'RSI_7', 'MACD_hist', 'MA5', 'MA20', 'MA50'],
            '4h': ['close', 'RSI_7', 'MACD_hist', 'MA5', 'MA20'],
            '1h': ['close', 'RSI_7', 'MACD_hist', 'MA5', 'MA10'],
            '15m': ['close', 'RSI_7', 'MACD_hist', 'MA5', 'MA10']
        }
        
        if not all(col in df.columns for col in required_columns[timeframe]):
            print(f"警告: {timeframe} 时间框架缺少必要的技术指标")
            return None

        # 删除空值并验证数据完整性
        df = df.dropna()
        if len(df) < limit:
            print(f"警告：{symbol} 在 {timeframe} 时间框架中的有效数据不足")
            return None

        return df.tail(limit)  # 只返回需要的数据点数

    except Exception as e:
        print(f"获取数据时出错 {symbol} {timeframe}: {str(e)}")
        return None

def check_conditions(df, timeframe):
    """
    根据不同时间框架检查是否满足筛选条件。
    """
    if df is None or df.empty:
        return False

    try:
        latest_data = {
            'close': df['close'].iloc[-1],
            'rsi': df['RSI_7'].iloc[-1],
            'macd_hist': df['MACD_hist'].iloc[-1]
        }

        if timeframe == '1d':
            return (
                latest_data['close'] > df['MA5'].iloc[-1] and
                latest_data['close'] > df['MA20'].iloc[-1] and
                latest_data['close'] > df['MA50'].iloc[-1] and
                latest_data['macd_hist'] > 0 and
                50 < latest_data['rsi'] < 70 and
                df['MA5'].iloc[-1] > df['MA5_prev'].iloc[-1] and
                df['MA20'].iloc[-1] > df['MA20_prev'].iloc[-1]
            )
        elif timeframe == '4h':
            return (
                latest_data['close'] > df['MA5'].iloc[-1] and
                latest_data['close'] > df['MA20'].iloc[-1] and
                latest_data['macd_hist'] > 0 and
                50 < latest_data['rsi'] < 70 and
                df['MA5'].iloc[-1] > df['MA5_prev'].iloc[-1] and
                df['MA20'].iloc[-1] > df['MA20_prev'].iloc[-1]
            )
        elif timeframe in ['1h', '15m']:
            return (
                df['MA5'].iloc[-1] > df['MA10'].iloc[-1] and
                latest_data['macd_hist'] > 0 and
                50 < latest_data['rsi'] < 75 and
                df['MA5'].iloc[-1] > df['MA5_prev'].iloc[-1] and
                df['MA10'].iloc[-1] > df['MA10_prev'].iloc[-1]
            )
        return False

    except Exception as e:
        print(f"检查条件时出错 {timeframe}: {str(e)}")
        return False

def filter_by_conditions(symbol, timeframes=['1d', '4h', '1h', '15m']):
    """
    检查指定交易对在不同时间框架下是否符合筛选条件。
    """
    satisfied_timeframes = 0
    results = {}

    for timeframe in timeframes:
        df = fetch_market_data(symbol, timeframe)
        if df is not None:
            is_satisfied = check_conditions(df, timeframe)
            results[timeframe] = is_satisfied
            if is_satisfied:
                satisfied_timeframes += 1

    if any(results.values()):
        print(f"\n{symbol} 各时间框架检查结果:")
        for tf, result in results.items():
            print(f"{tf}: {'满足' if result else '不满足'}")

    return satisfied_timeframes >= 3

def get_top_volume_perpetual(top_n=30):
    """
    获取成交量前top_n的U本位永续合约。
    """
    try:
        markets = exchange.load_markets()
        perpetual_pairs = [
            symbol for symbol, market in markets.items()
            if market['type'] == 'swap' and market['quote'] == 'USDT' and market['active']
        ]

        tickers = exchange.fetch_tickers(perpetual_pairs)
        market_data = [{
            'symbol': symbol,
            'base': markets[symbol]['base'],
            'last_price': tickers[symbol]['last']
        } for symbol in perpetual_pairs]

        df = pd.DataFrame(market_data).head(top_n)
        current_time = datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')

        print(f"\n获取时间（北京时间）: {current_time}")
        print(f"\n成交量前 {top_n} 的U本位永续合约:")
        print(df[['symbol', 'base', 'last_price']])

        return df, current_time

    except Exception as e:
        print(f"获取市场数据时出错: {str(e)}")
        return None, None

def main():
    try:
        df, current_time = get_top_volume_perpetual()
        if df is not None and not df.empty:
            # 筛选符合条件的交易对
            positive_symbols = [symbol for symbol in df['symbol'] if filter_by_conditions(symbol)]

            # 打印和保存结果
            if positive_symbols:
                print("\n同时满足筛选条件的交易对:")
                for symbol in positive_symbols:
                    print(symbol)

                # 保存结果
                result_df = pd.DataFrame({
                    'symbol': positive_symbols,
                    'beijing_time': current_time
                })
                filename = f"screened_symbols_{int(datetime.now().timestamp())}.csv"
                result_df.to_csv(filename, index=False)
                print(f"\n筛选结果已保存至: {filename}")
            else:
                print("\n没有交易对满足所有筛选条件。")
    except Exception as e:
        print(f"运行过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main()
