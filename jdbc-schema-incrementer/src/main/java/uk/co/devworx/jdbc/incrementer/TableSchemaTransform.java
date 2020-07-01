package uk.co.devworx.jdbc.incrementer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class describes what steps needs to be performed in
 * order to transform from one TableSchemaDescriptor to another.
 */
public class TableSchemaTransform
{
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
		TableMutation
	}

	private final Optional<TableSchemaDescriptor> before;
	private final TableSchemaDescriptor after;
	private final List<TableSchemaTransformStep> transformSteps;
	private final TransformType transformType;

	TableSchemaTransform(TableSchemaDescriptor after)
	{
		before = null;
		this.after = after;
		this.transformType = TransformType.NewTable;
		this.transformSteps = Collections.emptyList();
	}

	TableSchemaTransform(TableSchemaDescriptor beforeP, TableSchemaDescriptor after)
	{

		this.before = Optional.of(beforeP);
		this.after = after;
		this.transformSteps = new ArrayList<>();
		this.transformType = TransformType.TableMutation;
	}

	public Optional<TableSchemaDescriptor> getBefore()
	{
		return before;
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
