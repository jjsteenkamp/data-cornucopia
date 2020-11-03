package uk.co.devworx.spark.xsdschema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * The Overall container of the member paths.
 */
public class MemberPaths implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final Class<?> rootClass;
	private final List<MemberPath> memberPaths;
	private volatile transient StructType structType;

	public MemberPaths(Class<?> rootClass)
	{
		this.rootClass = rootClass;
		this.memberPaths = new CopyOnWriteArrayList<>();
	}

	public Class<?> getRootClass()
	{
		return rootClass;
	}

	void addMemberPaths(List<MemberPath> memberPathsP)
	{
		memberPaths.clear();
		memberPaths.addAll(memberPathsP);
	}

	public List<MemberPath> getMemberPaths()
	{
		return Collections.unmodifiableList(memberPaths);
	}

	public StructType getStructType()
	{
		if(structType == null)
		{
			List<StructField> allStructFields = memberPaths.stream().map(m -> m.createStructField()).collect(Collectors.toList());
			structType = DataTypes.createStructType(allStructFields);
		}
		return structType;
	}

	public Object[] createRowArray(final XSDSparkSchemaService service, final Object subject)
	{
		Objects.requireNonNull(subject, "You cannot pass in a null argument to this function ! ");
		final Class<?> providedClass = subject.getClass();
		if(providedClass.equals(rootClass) == false)
		{
			throw new IllegalArgumentException("You have provided this method with the class type : " + providedClass + " - it does not match the root class : " + rootClass);
		}

		Object[] rowValues = new Object[memberPaths.size()];
		for (int i = 0; i < memberPaths.size(); i++)
		{
			MemberPath memberPath = memberPaths.get(i);
			rowValues[i] = memberPath.extractRowValue(service, subject);
		}

		return rowValues;
	}

	public Row createRow(final XSDSparkSchemaService service, final Object subject)
	{
		return RowFactory.create(createRowArray(service, subject));
	}

	MemberPath findMatchingLevelDownMemberPath(MemberPath memberPathInput)
	{
		final List<String> attributePath = memberPathInput.getAttributePath();
		if(attributePath.size() <= 1)
		{
			throw new IllegalArgumentException("Cannot determine the level down path - as the attribute list is 1 or less. The Input Member Path was : \n " + memberPathInput);
		}

		final List<String> attributePathLevelDown = new ArrayList<>(attributePath);
		attributePathLevelDown.remove(0);

		//Now find a match amongst the members.

		List<MemberPath> memberPaths = getMemberPaths();
		for(MemberPath mp : memberPaths)
		{
			if(mp.getAttributePath().equals(attributePathLevelDown))
			{
				return mp;
			}
		}

		StringBuilder errorReport = new StringBuilder();
		errorReport.append("Could not find any level down matching paths for attributes - " + attributePath + " - root class : " + getRootClass() + ". Candidates included: \n");
		for(MemberPath mp : memberPaths)
		{
			errorReport.append(mp.getAttributePath());
			errorReport.append("\n");
		}
		throw new RuntimeException(errorReport.toString());
	}
}
