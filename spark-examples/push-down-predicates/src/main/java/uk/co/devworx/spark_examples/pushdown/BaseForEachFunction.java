package uk.co.devworx.spark_examples.pushdown;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Closeable;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static uk.co.devworx.spark_examples.pushdown.ExecutionEnv.GLOBAL_TMP_PREFIX;

/**
 * The base for each function
 */
public abstract class BaseForEachFunction implements ForeachFunction<Row>, Serializable, Closeable
{
	private final String baseName;
	private final Set<Class<?>> resultTypes;
	private final ExecutionEnv.EnvType envType;

	private transient volatile ExecutionEnv _env; //Don't access directly

	private final Map<Class<?>, String> resultNames;
	private final Map<Class<?>, List<?>> resultBuffers;

	public BaseForEachFunction(String baseName,
							   Class<?>... resultTypes)
	{
		this(ExecutionEnv.getDefaultEnvType(), baseName, resultTypes);
	}

	public BaseForEachFunction(ExecutionEnv.EnvType envType,
							   String baseName,
							   Class<?>... resultTypes)
	{
		this.envType = envType;
		this.baseName = baseName;
		this.resultTypes = new HashSet<>(Arrays.asList(resultTypes));
		this.resultNames = new ConcurrentHashMap<>(resultTypes.length);
		this.resultBuffers = new ConcurrentHashMap<>(resultTypes.length);

		for(Class<?> cls : resultTypes)
		{
			resultNames.put(cls, getClassDatasetName(cls));
			resultBuffers.put(cls, new ArrayList<>(getExecutionEnv().getBufferSize()));
		}
	}

	public final String getClassDatasetName(Class<?> type)
	{
		assertResultClassCorrect(type);
		String name = ExecutionEnv.getDatasetName(baseName, type);
		return name;
	}

	public final ExecutionEnv getExecutionEnv()
	{
		if(_env != null) return _env;
		synchronized (baseName)
		{
			if(_env != null) return _env;
			_env = ExecutionEnv.getInstance(envType);
			return _env;
		}
	}

	public final <T> Dataset<Row> getResultDataSet(Class<T> type)
	{
		assertResultClassCorrect(type);

		SparkSession session = getExecutionEnv().getSparkSession();

		String name = getClassDatasetName(type);

		if(session.catalog().databaseExists(GLOBAL_TMP_PREFIX) == false || session.catalog().tableExists(name) == false)
		{
			//Create a new item.
			List<T> buffer = (List)resultBuffers.get(type);
			getExecutionEnv().newDataset(baseName, buffer);
		}
		return session.table(name);
	}

	public final <T> void addResult(T result )
	{
		addResult((Class)result.getClass(), result);
	}

	public final <T> void addResult(Class<T> type, T result )
	{
		assertResultClassCorrect(type);
		List<T> buffer = (List)resultBuffers.get(type);
		buffer.add(result);
		if(buffer.size() >= buffer.size())
		{
			flushBuffers(type);
		}
	}

	protected <T> void flushBuffers(Class<T> type)
	{
		assertResultClassCorrect(type);

		Dataset<Row> table = getResultDataSet(type);

		List<T> buffer = (List)resultBuffers.get(type);
		List<T> bufferCopy = new ArrayList<>(buffer);
		buffer.clear();

		table.show();

	}

	private final void assertResultClassCorrect(Class<?> type)
	{
		if(resultTypes.contains(type) == false)
		{
			throw new IllegalArgumentException("Unrecognised return type - " + type.getSimpleName() + " -  you can only use these classes : " + resultTypes);
		}
	}

	@Override public void close()
	{

	}



}

