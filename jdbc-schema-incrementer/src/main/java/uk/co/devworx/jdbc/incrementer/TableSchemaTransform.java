package uk.co.devworx.jdbc.incrementer;


import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * This class describes what steps needs to be performed in
 * order to transform from one TableSchemaDescriptor to another.
 */
public class TableSchemaTransform
{
	private static final Logger logger = LogManager.getLogger(TableSchemaTransform.class);

	public static final String DDL_STEP_TABLE_CONVERSION_ASSERTS = "Table Conversion Asserts";
	public static final String DDL_STEP_DROP_IF_EXISTS = "Drop Temporary Table If Exists";
	public static final String DDL_STEP_CREATE_TEMP_TABLE = "Create Temporary Table";
	public static final String DDL_STEP_COPY_OVER_DATA = "Copy Over Data";
	public static final String DDL_STEP_RENAME_EXISTING_TABLE_TO_OLD = "Rename Existing Table to Old";
	public static final String DDL_STEP_RENAME_TMP_TABLE_TO_NEW = "Rename Tmp Table to New";
	public static final String DDL_STEP_DROP_PREVIOUS_IF_EXISTS = "Drop Previous Temporary Table";

	public static enum TransformType
	{
		/**
		 * A completely new table being created means that there were no prior tables
		 * that need to be updated as a result.
		 */
		NewTable,
		/**
		 * A before an after table exists - i.e. there
		 * has to be a step of SQL Statements that need to
		 * be performed to get from one to the other.
		 */
		TableMutation,

		/**
		 * Transform type indicating that there are no changes to the given table.
		 */
		NoChanges
	}

	public static enum TransformAnalysisResult
	{
		/**
		 * This particular table transform can be performed. There is no logic
		 * reason it should not work.
		 */
		Attested,

		/**
		 * This particular table transform can be performed. However there are some potential for
		 * failure. E.g. the transform of data types may not be convertable at runtime.
		 */
		FailurePotential,

		/**
		 * It is unclear how this particular table transformation should be performed
		 * as there are some logical clashes. Hence the system will reject it.
		 */
		Ambiguous,
	}

	private final Optional<TableSchemaDescriptor> beforeOpt;
	private final TableSchemaDescriptor after;
	private final List<TableSchemaTransformStep> transformSteps;
	private final TransformType transformType;
	private final TransformAnalysisResult analysisResult;

	private final SortedSet<TableColumn> newColumns;
	private final SortedSet<TableColumn> removedColumns;
	private final SortedSet<TableColumn> columnsWithChangedTypes;

	TableSchemaTransform(TableSchemaDescriptor after)
	{
		beforeOpt = Optional.empty();
		this.after = after;
		this.transformType = TransformType.NewTable;
		this.transformSteps = new ArrayList<>();
		this.analysisResult = TransformAnalysisResult.Attested;

		newColumns = new ConcurrentSkipListSet<>(after.getColumns().values());
		removedColumns = Collections.emptySortedSet();
		columnsWithChangedTypes = Collections.emptySortedSet();

		for(TableColumn newCol : newColumns)
		{
			transformSteps.add(new TableSchemaTransformStep(Optional.empty(),
															Optional.of(newCol),
															TableSchemaTransformStep.StepType.InsertedColumn,
															newCol.getOrdinal()));
		}

	}

	TableSchemaTransform(TableSchemaDescriptor beforeP, TableSchemaDescriptor after)
	{
		this.beforeOpt = Optional.of(beforeP);
		this.after = after;
		this.transformSteps = new ArrayList<>();

		//Ok, now we need to do the differences calc.
		final Map<TableColumn, Integer> newOrdinals = new HashMap<>();

		final TableSchemaDescriptor before = beforeP;
		final Map<String, TableColumn> beforeColumns = before.getColumns();
		final Map<String, TableColumn> afterColumns = after.getColumns();

		beforeColumns.forEach((k,v) ->
        {
			TableColumn afterColumn = afterColumns.get(k);
			if(afterColumn != null)
			{
				newOrdinals.put(v, afterColumn.getOrdinal());
			}
		});

		newColumns = findNewColumns(beforeOpt.get(), after);
		removedColumns = findRemovedColumns(beforeOpt.get(), after);
		columnsWithChangedTypes = findColumnsChangedTypes(beforeOpt.get(), after);

		if(newColumns.isEmpty() && removedColumns.isEmpty() && columnsWithChangedTypes.isEmpty())
		{
			this.transformType = TransformType.NoChanges;
		}
		else
		{
			this.transformType = TransformType.TableMutation;
		}

		if(newColumns.isEmpty() == false && removedColumns.isEmpty() == false)
		{
			analysisResult = TransformAnalysisResult.Ambiguous;
		}
		else if(SQLDataTypeHelper.areChangedColumnTypesFailurePotentials(after, columnsWithChangedTypes))
		{
			analysisResult = TransformAnalysisResult.FailurePotential;
		}
		else
		{
			analysisResult = TransformAnalysisResult.Attested;
		}

		logger.info("ANALYSIS RESULT : " + analysisResult);

		if(analysisResult.equals(TransformAnalysisResult.Ambiguous)) return;

		//Right, now we can produce the transformation type scripts.

		final Map<String, TableColumn> beforeColumnsMap = before.getColumns();
		final Map<String, TableColumn> afterColumnsMap = after.getColumns();
		final ArrayList<TableColumn> afterColumnsSorted = new ArrayList<>(new ConcurrentSkipListSet<>(afterColumnsMap.values()));

		//Add all the removed columns!
		for(TableColumn rmdCol : removedColumns)
		{
			transformSteps.add(new TableSchemaTransformStep(Optional.of(rmdCol),
															Optional.empty(),
															TableSchemaTransformStep.StepType.RemovedColumn,
															0));
		}

		//Now do the other ones

		for(int i=0; i<afterColumnsSorted.size(); i++)
		{
			final int ordinal = (i + 1);
			final TableColumn afterColumn = afterColumnsSorted.get(i);
			if(newColumns.contains(afterColumn) == true)
			{
				transformSteps.add(new TableSchemaTransformStep(Optional.empty(),
																Optional.of(afterColumn),
																TableSchemaTransformStep.StepType.InsertedColumn,
																ordinal));
			}
			else if(columnsWithChangedTypes.contains(afterColumn) == true) // We have a type change
			{
				//An existing column, like for like...
				transformSteps.add(new TableSchemaTransformStep(Optional.of(beforeColumnsMap.get(afterColumn.getColumnName())),
																Optional.of(afterColumn),
																TableSchemaTransformStep.StepType.TypeChangedColumn,
																ordinal));
			}
			else
			{
				//We have no change
				transformSteps.add(new TableSchemaTransformStep(Optional.of(beforeColumnsMap.get(afterColumn.getColumnName())),
																Optional.of(afterColumn),
																TableSchemaTransformStep.StepType.NoChange,
																ordinal));
			}
		}
	}

	/**
	 * Simply calls generateCreateDDLSQL() - an d just creates a single summary
	 * string script with all the code. Cannot be executed directly though.
	 *
	 * @return
	 */
	public String getSQLExecReport()
	{
		final StringBuilder report = new StringBuilder();
		final LinkedHashMap<String, String> sqls = generateCreateDDLSQL();

		sqls.forEach((k,v) ->
		{
			report.append(k + "\n-----------------------------\n");
			report.append(v + "\n\n");
		});

		return report.toString();
	}

	/**
	 * Generate the list of named SQL steps that need to be performed in order to
	 * create the new table and copy of the existing data into it (with NULLS) on
	 * columns where appropriate
	 *
	 * @return
	 */
	public LinkedHashMap<String, String> generateCreateDDLSQL()
	{
		final LinkedHashMap<String, String> res = new LinkedHashMap<String, String>();
		final Optional<CreateTableScriptsUtil.CreateTransientTableScript> transScriptOpt = after.getTransientCreateScript();

		if(transScriptOpt.isPresent() == false)
		{
			throw new RuntimeException("Unable to generate a SQL DDL Script - as the 'after' table descriptor does not actually have a transient create script !");
		}

		final CreateTableScriptsUtil.CreateTransientTableScript transScript = transScriptOpt.get();
		final String tmpTableName = transScript.getResolvedTransientTableSchema().get() + "." + transScript.getResolvedTransientTableName().get();
		final String beforeTableName = getBefore().get().getSchema() + "." + getBefore().get().getRootName();

		//Any transform assert scripts
		StringBuilder assertTransforms = new StringBuilder();
		for(TableSchemaTransformStep step : transformSteps)
		{
			if(step.getStepType().equals(TableSchemaTransformStep.StepType.TypeChangedColumn))
			{
				assertTransforms.append("SELECT " + SQLDataTypeHelper.generateColumnTransformCode(step.getBeforeColumn().get(), step.getAfterColumn().get()) +
										" AS " + step.getAfterColumn().get().getColumnName());
				assertTransforms.append(" FROM " + beforeTableName + " ; \n");
			}
		}
		if(assertTransforms.length() > 0 )
		{
			res.put(DDL_STEP_TABLE_CONVERSION_ASSERTS, assertTransforms.toString());
		}




		res.put(DDL_STEP_DROP_IF_EXISTS, "DROP TABLE IF EXISTS " + tmpTableName + " ;");
		res.put(DDL_STEP_CREATE_TEMP_TABLE, transScript.getResolvedCreateTableScript().get());

		//Now copy over the data ....
		StringBuilder copyOverData = new StringBuilder();
		copyOverData.append("INSERT INTO " + tmpTableName + " \n ");
		copyOverData.append("SELECT ");
		for(TableSchemaTransformStep step : transformSteps)
		{
			switch(step.getStepType())
			{
			case InsertedColumn:
				copyOverData.append("\n\t\t NULL AS " + step.getAfterColumn().get().getColumnName());
				copyOverData.append(",");
				break;
			case NoChange:
				copyOverData.append("\n\t\t " + step.getAfterColumn().get().getColumnName());
				copyOverData.append(",");
				break;
			case TypeChangedColumn:
				copyOverData.append("\n\t\t " + SQLDataTypeHelper.generateColumnTransformCode(step.getBeforeColumn().get(), step.getAfterColumn().get()) +
											" AS " + step.getAfterColumn().get().getColumnName());
				copyOverData.append(",");
				break;
			case RemovedColumn:
				copyOverData.append("\n-- REMOVED Column : \t\t " + step.getBeforeColumn().get().getColumnName());
				break;
			default:
				throw new RuntimeException("Unknown step type - " + step.getStepType());
			}
		}
		copyOverData.deleteCharAt(copyOverData.length() - 1);
		copyOverData.append("\n");
		copyOverData.append(" FROM " + beforeTableName);

		res.put(DDL_STEP_COPY_OVER_DATA, copyOverData.toString());

		res.put(DDL_STEP_RENAME_EXISTING_TABLE_TO_OLD, " ALTER TABLE " + beforeTableName + " RENAME TO " + tmpTableName + "_previous ;");
		res.put(DDL_STEP_RENAME_TMP_TABLE_TO_NEW, " ALTER TABLE " + tmpTableName + " RENAME TO " + beforeTableName + " ;");
		res.put(DDL_STEP_DROP_PREVIOUS_IF_EXISTS, " DROP TABLE " + tmpTableName + "_previous ;");

		return res;
	}

	public TransformAnalysisResult getAnalysisResult()
	{
		return analysisResult;
	}

	public String getChangeFindingsReport()
	{
		StringBuilder report = new StringBuilder();
		report.append("\n");
		report.append("\n");
		report.append("Changes - Before / After for: " + after.getRootName() + " - Mutation type : " + transformType);
		report.append("\n");

		report.append("Total New Columns -> " + newColumns.size() + "\n");
		report.append("Total Removed Columns -> " + removedColumns.size() + "\n");
		report.append("Total Columns With Changed Types -> " + columnsWithChangedTypes.size() + "\n");
		report.append("\n");
		report.append("New Columns\n");
		report.append("------------\n");

		final AsciiTable newColumnsAsciiTable = new AsciiTable();
		newColumnsAsciiTable.addRule();
		newColumnsAsciiTable.addRow("Table Name", "Column Name", "SQL Column Type", "Ordinal");
		newColumnsAsciiTable.addRule();

		newColumns.forEach(c -> newColumnsAsciiTable.addRow(c.getParent().getRootName(), c.getColumnName(), c.getSqlColumnTypeName(), c.getOrdinal()));

		newColumnsAsciiTable.addRule();
		CWC_LongestLine cwc = new CWC_LongestLine();
		newColumnsAsciiTable.getRenderer().setCWC(cwc);
		report.append(newColumnsAsciiTable.render());

		report.append("\n");report.append("\n");
		report.append("Removed Columns\n");
		report.append("------------\n");

		final AsciiTable removedColumnsAsciiTable = new AsciiTable();
		removedColumnsAsciiTable.addRule();
		removedColumnsAsciiTable.addRow("Table Name", "Column Name", "SQL Column Type", "Ordinal");
		removedColumnsAsciiTable.addRule();

		removedColumns.forEach(c -> removedColumnsAsciiTable.addRow(c.getParent().getRootName(), c.getColumnName(), c.getSqlColumnTypeName(), c.getOrdinal()));
		removedColumnsAsciiTable.addRule();
		cwc = new CWC_LongestLine();
		removedColumnsAsciiTable.getRenderer().setCWC(cwc);
		report.append(removedColumnsAsciiTable.render());

		report.append("\n");report.append("\n");
		report.append("Type Changed Columns\n");
		report.append("------------\n");

		final AsciiTable typeChangesColumnsAsciiTable = new AsciiTable();
		typeChangesColumnsAsciiTable.addRule();
		typeChangesColumnsAsciiTable.addRow("Table Name", "Column Name", "Before Column Type", "After Column Type", "Ordinal");
		typeChangesColumnsAsciiTable.addRule();

		columnsWithChangedTypes.forEach(c ->
        {
			TableColumn targetCol = after.getColumns().get(c.getColumnName());
			String newDataType = targetCol != null ? targetCol.getSqlColumnTypeName() : "#ERROR";
			typeChangesColumnsAsciiTable.addRow(c.getParent().getRootName(), c.getColumnName(), c.getSqlColumnTypeName(), newDataType, c.getOrdinal());
		});

		typeChangesColumnsAsciiTable.addRule();
		cwc = new CWC_LongestLine();
		typeChangesColumnsAsciiTable.getRenderer().setCWC(cwc);
		report.append(typeChangesColumnsAsciiTable.render());
		report.append("\n");

		return report.toString();
	}

	public String getTransformStepsReport()
	{
		StringBuilder report = new StringBuilder();
		report.append("\n");
		report.append("\n");
		report.append("Transform Steps - Before / After for: " + after.getRootName() + " - Mutation type : " + transformType + " | Analysis Result : " + getAnalysisResult());
		report.append("\n");
		report.append("Total Steps  -> " + transformSteps.size() + "\n");
		report.append("\n");

		final AsciiTable transtepsTable = new AsciiTable();
		transtepsTable.addRule();
		transtepsTable.addRow("Ordinal", "Transform Type", "Before Column", "Before Column Type", "After Column", "After Column Type");
		transtepsTable.addRule();

		for(TableSchemaTransformStep step : transformSteps)
		{
			transtepsTable.addRow(step.getOrdinal(),
								  step.getStepType(),
								  step.getBeforeColumn().isPresent() ? step.getBeforeColumn().get().getColumnName() : "",
								  step.getBeforeColumn().isPresent() ? step.getBeforeColumn().get().getSqlColumnTypeName() : "",
								  step.getAfterColumn().isPresent() ? step.getAfterColumn().get().getColumnName() : "",
								  step.getAfterColumn().isPresent() ? step.getAfterColumn().get().getSqlColumnTypeName() : ""
								 );
		}
		transtepsTable.addRule();
		CWC_LongestLine cwc = new CWC_LongestLine();
		transtepsTable.getRenderer().setCWC(cwc);
		report.append(transtepsTable.render());
		report.append("\n");
		return report.toString();
	}

	private SortedSet<TableColumn> findColumnsChangedTypes(TableSchemaDescriptor before, TableSchemaDescriptor after)
	{
		final SortedSet<TableColumn> columnChangedTypes = new ConcurrentSkipListSet<>();

		final Map<String, TableColumn> beforeCols = before.getColumns();
		final Map<String, TableColumn> afterCols = after.getColumns();

		beforeCols.forEach((k,v) ->
		{
			TableColumn tablCol = afterCols.get(k);
			if (tablCol != null)
			{
				if(tablCol.getSqlColumnTypeName().equals(v.getSqlColumnTypeName()) == false)
				{
					columnChangedTypes.add(tablCol);
				}
			}
		});

		return columnChangedTypes;
	}

	static final SortedSet<TableColumn> findRemovedColumns(TableSchemaDescriptor before, TableSchemaDescriptor after)
	{
		final SortedSet<TableColumn> removeds = new ConcurrentSkipListSet<>();
		final Map<String, TableColumn> beforeCols = before.getColumns();
		final Map<String, TableColumn> afterCols = after.getColumns();
		beforeCols.forEach((k,v) ->
		{
			TableColumn tablCol = afterCols.get(k);
			if(tablCol == null)
			{
				removeds.add(v);
			}
		});

		return removeds;
	}

	static final SortedSet<TableColumn> findNewColumns(TableSchemaDescriptor before, TableSchemaDescriptor after)
	{
		final SortedSet<TableColumn> news = new ConcurrentSkipListSet<>();
		final Map<String, TableColumn> beforeCols = before.getColumns();
		final Map<String, TableColumn> afterCols = after.getColumns();
		afterCols.forEach((k,v) ->
		{
			TableColumn tablCol = beforeCols.get(k);
			if(tablCol == null) // This is a completely new column - not there
			{
				news.add(v);
			}
		});

		return news;
	}

	public Optional<TableSchemaDescriptor> getBefore()
	{
		return beforeOpt;
	}

	public TableSchemaDescriptor getAfter()
	{
		return after;
	}

	public List<TableSchemaTransformStep> getTransformSteps()
	{
		return Collections.unmodifiableList(transformSteps);
	}

	public TransformType getTransformType()
	{
		return transformType;
	}

}
