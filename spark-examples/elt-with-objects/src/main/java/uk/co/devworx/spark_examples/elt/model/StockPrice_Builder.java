package uk.co.devworx.spark_examples.elt.model;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;

/**
 * The builder object for a stock price. It produces an immutable stock price object. 
 *  
 * @author jsteenkamp
 */
public class StockPrice_Builder
{
	private String stockId;
	private String priceDate;
	private String lastOrMidPrice;
	private String open;
	private String high;
	private String low;
	private String close;
	private String volume;
	private String adjustedOpen;
	private String adjustedHigh;
	private String adjustedLow;
	private String adjustedClose;
	private String adjustedVolume;
	private String dividend;
	
	/**
	 * Builds a stock price object 
	 * @return
	 * @throws StockPrice_BuilderException
	 */
	public StockPrice build() throws StockPrice_BuilderException
	{
		List<BuildValidationFailure> failures = getValidationFailures();
		if(failures.isEmpty() == false) throw new StockPrice_BuilderException(failures);
		
		return new StockPrice(stockId, 
		                      LocalDate.parse(priceDate), 
		                      getOptionalDouble(lastOrMidPrice).getAsDouble(), 
		                      getOptionalDouble(open).getAsDouble(), 
		                      getOptionalDouble(high).getAsDouble(), 
		                      getOptionalDouble(low).getAsDouble(),
		                      getOptionalDouble(close).getAsDouble(), 
		                      getOptionalDouble(volume).getAsDouble(), 
		                      getOptionalDouble(adjustedOpen).getAsDouble(), 
		                      getOptionalDouble(adjustedHigh).getAsDouble(), 
		                      getOptionalDouble(adjustedLow).getAsDouble(), 
		                      getOptionalDouble(adjustedClose).getAsDouble(), 
		                      getOptionalDouble(adjustedVolume).getAsDouble(), 
		                      getOptionalDouble(dividend).getAsDouble());
	}
	
	/**
	 * Gets the failure (if any) from the given object if you were to try and build it directly.
	 * 
	 * @return
	 */
	public List<BuildValidationFailure> getValidationFailures()
	{
		final List<BuildValidationFailure> failures = new ArrayList<>();
		
		if(stockId == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "stockId", String.valueOf(stockId), "Empty/Null Value supplied"));
		}
		if(priceDate == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "priceDate", String.valueOf(priceDate), "Empty/Null Value supplied"));
		}
		else
		{
			try
			{
				LocalDate.parse(priceDate);
			}
			catch(Exception e)
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "priceDate", String.valueOf(priceDate), "Unable to parse the date value - got exception : " + e));
			}
		}
		
		if(lastOrMidPrice == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "lastOrMidPrice", String.valueOf(lastOrMidPrice), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(lastOrMidPrice).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "lastOrMidPrice", String.valueOf(lastOrMidPrice), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(open == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "open", String.valueOf(open), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(open).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "open", String.valueOf(open), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(high == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "high", String.valueOf(high), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(high).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "high", String.valueOf(high), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(low == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "low", String.valueOf(low), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(low).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "low", String.valueOf(low), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(close == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "close", String.valueOf(close), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(close).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "close", String.valueOf(close), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(volume == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "volume", String.valueOf(volume), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(volume).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "volume", String.valueOf(volume), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(adjustedOpen == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedOpen", String.valueOf(adjustedOpen), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(adjustedOpen).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedOpen", String.valueOf(adjustedOpen), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(adjustedHigh == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedHigh", String.valueOf(adjustedHigh), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(adjustedHigh).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedHigh", String.valueOf(adjustedHigh), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(adjustedLow == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedLow", String.valueOf(adjustedLow), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(adjustedLow).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedLow", String.valueOf(adjustedLow), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(adjustedClose == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedClose", String.valueOf(adjustedClose), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(adjustedClose).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedClose", String.valueOf(adjustedClose), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(adjustedVolume == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedVolume", String.valueOf(adjustedVolume), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(adjustedVolume).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "adjustedVolume", String.valueOf(adjustedVolume), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
		if(dividend == null) 
		{
			failures.add(BuildValidationFailure.newFailure(StockPrice.class, "dividend", String.valueOf(dividend), "Empty/Null Value supplied"));
		}
		else
		{
			getOptionalDouble(dividend).orElseGet(() -> 
			{
				failures.add(BuildValidationFailure.newFailure(StockPrice.class, "dividend", String.valueOf(dividend), "Unable to parse a double from the value"));
				return Double.NaN;
			});
		}
 
		return Collections.unmodifiableList(failures);
	}
	
	StockPrice_Builder()
	{
	}

	public String getStockId()
	{
		return stockId;
	}

	public StockPrice_Builder setStockId(String stockId)
	{
		this.stockId = stockId;
		return this;
	}

	public String getPriceDate()
	{
		return priceDate;
	}

	public StockPrice_Builder setPriceDate(String priceDate)
	{
		this.priceDate = priceDate;
		return this;
	}

	public String getLastOrMidPrice()
	{
		return lastOrMidPrice;
	}

	public StockPrice_Builder setLastOrMidPrice(String lastOrMidPrice)
	{
		this.lastOrMidPrice = lastOrMidPrice;
		return this;
	}

	public String getOpen()
	{
		return open;
	}

	public StockPrice_Builder setOpen(String open)
	{
		this.open = open;
		return this;
	}

	public String getHigh()
	{
		return high;
	}

	public StockPrice_Builder setHigh(String high)
	{
		this.high = high;
		return this;
	}

	public String getLow()
	{
		return low;
	}

	public StockPrice_Builder setLow(String low)
	{
		this.low = low;
		return this;
	}

	public String getClose()
	{
		return close;
	}

	public StockPrice_Builder setClose(String close)
	{
		this.close = close;
		return this;
	}

	public String getVolume()
	{
		return volume;
	}

	public StockPrice_Builder setVolume(String volume)
	{
		this.volume = volume;
		return this;
	}

	public String getAdjustedOpen()
	{
		return adjustedOpen;
	}

	public StockPrice_Builder setAdjustedOpen(String adjustedOpen)
	{
		this.adjustedOpen = adjustedOpen;
		return this;
	}

	public String getAdjustedHigh()
	{
		return adjustedHigh;
	}

	public StockPrice_Builder setAdjustedHigh(String adjustedHigh)
	{
		this.adjustedHigh = adjustedHigh;
		return this;
	}

	public String getAdjustedLow()
	{
		return adjustedLow;
	}

	public StockPrice_Builder setAdjustedLow(String adjustedLow)
	{
		this.adjustedLow = adjustedLow;
		return this;
	}

	public String getAdjustedClose()
	{
		return adjustedClose;
	}

	public StockPrice_Builder setAdjustedClose(String adjustedClose)
	{
		this.adjustedClose = adjustedClose;
		return this;
	}

	public String getAdjustedVolume()
	{
		return adjustedVolume;
	}

	public StockPrice_Builder setAdjustedVolume(String adjustedVolume)
	{
		this.adjustedVolume = adjustedVolume;
		return this;
	}

	public String getDividend()
	{
		return dividend;
	}

	public StockPrice_Builder setDividend(String dividend)
	{
		this.dividend = dividend;
		return this;
	}
	
	/**
	 * Attemts to parse the given double.
	 * 
	 * @return
	 */
	private OptionalDouble getOptionalDouble(String fieldValue)
	{
		try
		{
			return OptionalDouble.of(Double.parseDouble(fieldValue));
		}
		catch(NumberFormatException ne)
		{
			return OptionalDouble.empty();
		}
	}
	
	public static class StockPrice_BuilderException extends RuntimeException
	{
		private static final long serialVersionUID = 1L;
		
		private final List<BuildValidationFailure> failures;
		
		public StockPrice_BuilderException(List<BuildValidationFailure> failures)
		{
			this.failures = failures;
		}

		@Override
		public String toString()
		{
			return super.toString() + "\n" + BuildValidationFailure.summaryReport(failures);
		}
		
	}
	
}























