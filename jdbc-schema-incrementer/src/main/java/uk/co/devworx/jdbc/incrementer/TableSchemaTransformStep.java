package uk.co.devworx.jdbc.incrementer;

import java.util.Optional;

/**
 * Describes an individual schema transform step.
 * E.g. mapping one column to another etc.
 */
public class TableSchemaTransformStep
{

	public static enum StepType
	{
		NoChange,  //To indicate no change from the before to the after columns
		AppendedColumn,
		InsertedColumn,
		TypeChangedColumn,
		RemovedColumn,
		RenamedColumn
	}

	private final Optional<TableColumn> beforeColumn;
	private final Optional<TableColumn> afterColumn;
	private final StepType stepType;
	private final int ordinal;

	TableSchemaTransformStep(Optional<TableColumn> beforeColumn,
							 Optional<TableColumn> afterColumn,
							 StepType stepType,
							 int ordinal)
	{
		this.beforeColumn = beforeColumn;
		this.afterColumn = afterColumn;
		this.stepType = stepType;
		this.ordinal = ordinal;
	}

}
