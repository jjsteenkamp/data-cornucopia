package uk.co.devworx.spark_examples.elt.model;

import java.time.LocalDate;

/**
 * A stock price object representing the EOD snapshot 
 * 
 * @author jsteenkamp
 * 
 */
public class StockPrice
{
	public static StockPrice_Builder builder()
	{
		return new StockPrice_Builder();
	}
	
	private final String stockId;
	private final LocalDate priceDate;
	private final double lastOrMidPrice;
	private final double open;
	private final double high;
	private final double low;
	private final double close;
	private final double volume;
	private final double adjustedOpen;
	private final double adjustedHigh;
	private final double adjustedLow;
	private final double adjustedClose;
	private final double adjustedVolume;
	private final double dividend;
	
	StockPrice(String stockId, LocalDate priceDate, double lastOrMidPrice, double open, double high, double low, double close, double volume, double adjustedOpen, double adjustedHigh, double adjustedLow, double adjustedClose, double adjustedVolume, double dividend)
	{
		this.stockId = stockId;
		this.priceDate = priceDate;
		this.lastOrMidPrice = lastOrMidPrice;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.volume = volume;
		this.adjustedOpen = adjustedOpen;
		this.adjustedHigh = adjustedHigh;
		this.adjustedLow = adjustedLow;
		this.adjustedClose = adjustedClose;
		this.adjustedVolume = adjustedVolume;
		this.dividend = dividend;
	}
	
	/**
	 * The calculated Mid price 
	 * s
	 * @return
	 */
	public double getCalculatedMid()
	{
		return (high + low / 2.0);
	}
	
	/**
	 * The calculated Mid price 
	 * s
	 * @return
	 */
	public double getCalculatedAdjustedMid()
	{
		return (adjustedHigh + adjustedLow / 2.0);
	}
	
	public String getStockId()
	{
		return stockId;
	}

	public LocalDate getPriceDate()
	{
		return priceDate;
	}

	public double getLastOrMidPrice()
	{
		return lastOrMidPrice;
	}

	public double getOpen()
	{
		return open;
	}

	public double getHigh()
	{
		return high;
	}

	public double getLow()
	{
		return low;
	}

	public double getClose()
	{
		return close;
	}

	public double getVolume()
	{
		return volume;
	}

	public double getAdjustedOpen()
	{
		return adjustedOpen;
	}

	public double getAdjustedHigh()
	{
		return adjustedHigh;
	}

	public double getAdjustedLow()
	{
		return adjustedLow;
	}

	public double getAdjustedClose()
	{
		return adjustedClose;
	}

	public double getAdjustedVolume()
	{
		return adjustedVolume;
	}

	public double getDividend()
	{
		return dividend;
	}
	
	
}
