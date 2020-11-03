package uk.co.devworx.spark.xsdschema;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.EnumDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.ast.expr.MemberValuePair;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import javax.xml.bind.annotation.XmlElement;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The main service that will assist in the conversion of XSD types
 * into Spark Schemas
 */
public class XSDSparkSchemaServiceBuilder
{
	private static final Logger logger = LogManager.getLogger(XSDSparkSchemaServiceBuilder.class);

	/**
	 * The main method for generating the embedded binary.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String... args) throws Exception
	{
		if(args.length < 2) throw new IllegalArgumentException("Unable to create the schema service - as you haven't specified source inputs.");
		Path path = Paths.get(args[0]);
		if(Files.exists(path) == false || Files.isDirectory(path) == false)
		{
			throw new IllegalArgumentException("You must specify an input path that is a directory and that exists ! You specified : " + args[0] + " - which equates to : " + path.toAbsolutePath());
		}

		Path pathOutput = Paths.get(args[1]);
		Files.createDirectories(pathOutput.getParent());

		XSDSparkSchemaServiceBuilder bldr = new XSDSparkSchemaServiceBuilder(path);
		XSDSparkSchemaService xsdSparkSchemaService = bldr.buildServiceForFullTypes();

		try(OutputStream fileOut = Files.newOutputStream(pathOutput);
			ObjectOutputStream ous = new ObjectOutputStream(fileOut))
		{
			ous.writeObject(xsdSparkSchemaService);
		}

		logger.info("All Done - Existing");
	}


	public static final Set<String> CLASS_NAMES_TO_SKIP;
	static
	{
		HashSet cn = new HashSet<>();
		cn.add("ObjectFactory");
		CLASS_NAMES_TO_SKIP = cn;
	}


	public static final Set<String> MEMBER_NAMES_TO_SKIP;
	static
	{
		HashSet ms = new HashSet<>();
		ms.add("serialVersionUID");
		MEMBER_NAMES_TO_SKIP = ms;
	}

	private final Path sourceInputDir;
	private final List<Path> allJavaSources;

	private final Map<Class<?>, Path> topLevelClassesToSourcePaths;
	private final Map<Class<?>, ClassOrInterfaceDeclaration> topLevelClassDeclarations;
	private final Map<Class<?>, Path> enumsToSourcePaths;
	private final Map<Class<?>, MemberPaths> classMemberPaths;

	XSDSparkSchemaServiceBuilder(Path sourceInputDirP) throws IOException
	{
		this.sourceInputDir = sourceInputDirP;
		this.allJavaSources = getAllJavaSources(sourceInputDir);

		classMemberPaths = new ConcurrentHashMap<>();
		topLevelClassesToSourcePaths = new ConcurrentHashMap<>();
		enumsToSourcePaths = new ConcurrentHashMap<>();
		topLevelClassDeclarations = new ConcurrentHashMap<>();

		for(Path p : allJavaSources)
		{
			final CompilationUnit comUnit = (new JavaParser().parse(p)).getResult().get();
			final List<ClassOrInterfaceDeclaration> allClassOrIntfDeclr = comUnit.findAll(ClassOrInterfaceDeclaration.class);

			for (ClassOrInterfaceDeclaration cd : allClassOrIntfDeclr)
			{
				Optional<PackageDeclaration> pkgDeclrOpt = comUnit.getPackageDeclaration();
				if(pkgDeclrOpt.isPresent() == false)
				{
					throw new RuntimeException("Could not Find Package Declr Node : " + p);
				}

				if(CLASS_NAMES_TO_SKIP.contains(cd.getNameAsString()))
				{
					continue;
				}

				String clazzName = pkgDeclrOpt.get().getNameAsString() + "." + cd.getNameAsString();
				if(cd.isPublic() && cd.isStatic() == true)
				{
					Optional<Node> parentNodeOpt = cd.getParentNode();
					Node parentNode = parentNodeOpt.get();
					if(parentNode instanceof ClassOrInterfaceDeclaration)
					{
						ClassOrInterfaceDeclaration enclosingClass = (ClassOrInterfaceDeclaration)parentNode;
						String parentClassName = pkgDeclrOpt.get().getNameAsString() + "." + enclosingClass.getNameAsString();
						clazzName = parentClassName + "$" + cd.getNameAsString();
					}
				}

				try
				{
					Class<?> targetClass = Class.forName(clazzName);
					topLevelClassesToSourcePaths.put(targetClass, p);
					topLevelClassDeclarations.put(targetClass, cd);
				}
				catch (ClassNotFoundException e)
				{
					throw new RuntimeException("Unable to find the class  : " + clazzName + " | Item : " + e );
				}
			}

			List<EnumDeclaration> allEnums = comUnit.findAll(EnumDeclaration.class);
			for (EnumDeclaration e : allEnums)
			{
				Optional<PackageDeclaration> pkgDeclrOpt = comUnit.getPackageDeclaration();
				if(pkgDeclrOpt.isPresent() == false)
				{
					throw new RuntimeException("Could not Find Package Declr Node : " + p);
				}

				String clazzName = pkgDeclrOpt.get().getNameAsString() + "." + e.getNameAsString();
				try
				{
					enumsToSourcePaths.put(Class.forName(clazzName), p);
				}
				catch (ClassNotFoundException xe)
				{
					throw new RuntimeException("Unable to find the class  : " + clazzName + " | Item : " + xe );
				}
			}
		}
	}

	public XSDSparkSchemaService buildServiceForFullTypes()
	{
		topLevelClassesToSourcePaths.keySet().forEach(c -> getMemberPath(c));
		return ___buildService();
	}

	private XSDSparkSchemaService ___buildService()
	{
		Map<Class<?>, MemberPaths> inpsd = new ConcurrentHashMap<>();
		inpsd.putAll(classMemberPaths);

		Set<Class<?>> allEnumTypes = Collections.newSetFromMap(new ConcurrentHashMap<>());
		allEnumTypes.addAll(enumsToSourcePaths.keySet());

		return new XSDSparkSchemaService(Collections.unmodifiableMap(inpsd),
										 Collections.unmodifiableSet(allEnumTypes));
	}

	public XSDSparkSchemaService buildServiceForTypes(Class... inputClasses)
	{
		for (int i = 0; i < inputClasses.length; i++)
		{
			getMemberPath(inputClasses[i]);
		}

		return ___buildService();
	}

	private MemberPaths getMemberPath(Class<?> inputClass)
	{
		MemberPaths memberPaths = classMemberPaths.get(inputClass);
		if(memberPaths != null)
		{
			return memberPaths;
		}
		memberPaths = ____getMemberPath(inputClass);
		classMemberPaths.put(inputClass, memberPaths);
		return memberPaths;
	}

	private MemberPaths ____getMemberPath(Class<?> inputClass)
	{
		ClassOrInterfaceDeclaration cd = topLevelClassDeclarations.get(inputClass);
		if(cd == null)
		{
			throw new RuntimeException("The input class you have specified : " + inputClass + " - is not part of the universe of available classes");
		}

		final List<FieldDeclaration> fields = cd.getFields();
		final List<MemberPath> memberPaths = new ArrayList<>();

		logger.info("Input Class :: " + inputClass);

		int ordinal = 0;

		for (FieldDeclaration field : fields)
		{
			NodeList<VariableDeclarator> variables = field.getVariables();
			Optional<VariableDeclarator> optVariable = variables.getFirst();
			if(optVariable.isPresent() == false)
			{
				throw new RuntimeException("Unexpected - field : " + field + " - does not have a variable declarion");
			}

			VariableDeclarator variable = optVariable.get();
			String variableName = variable.getName().getIdentifier();

			if(MEMBER_NAMES_TO_SKIP.contains(variableName)) continue;

			final String xmlAnnotationName = extractXmlAnnotationName(variableName, field);

			ClassOrInterfaceType attributeType = (ClassOrInterfaceType)variable.getType();
			Class<?> referencedClass = XSDSparkSchemaService.determineFullClass(inputClass, attributeType.getNameWithScope());

			//Ok, so lets see what it is.
			if(enumsToSourcePaths.containsKey(referencedClass) == true)
			{
				//This is an enum that is referenced.
				DataType targetType = DataTypes.StringType;
				memberPaths.add(new MemberPath(inputClass,
											   ordinal,
											   Collections.singletonList(variableName),
											   targetType,
											   referencedClass,
											   xmlAnnotationName));
			}
			else if(topLevelClassDeclarations.containsKey(referencedClass) == true)
			{
				if(referencedClass.equals(inputClass) == true)
				{
					throw new RuntimeException("Found the inputClass - " + inputClass + " to reference itself directly, hence causing a cycle - cannot continue !");
				}
				final MemberPaths referencedMemberPaths = getMemberPath(referencedClass);
				for(MemberPath underlyingPath : referencedMemberPaths.getMemberPaths())
				{
					memberPaths.add(underlyingPath.cloneWithPrefixAndRootType(inputClass, variableName, ordinal));
					ordinal++;
				}
			}
			else if(referencedClass.equals(List.class))
			{
				// We have a list type.... need to make this a very specific array
				final Optional<NodeList<Type>> typeArgumentsOpt = attributeType.getTypeArguments();
				if(typeArgumentsOpt.isPresent() == false)
				{
					throw new RuntimeException("Unable to resolve the type arguments for the list variable " + variableName + " - InputClass : " + inputClass);
				}

				final NodeList<Type> listGenericTypes = typeArgumentsOpt.get();
				if(listGenericTypes.size() != 1)
				{
					throw new RuntimeException("Unable to resolve the type arguments for the list variable " + variableName + " - InputClass : " + inputClass + " - the type argument list is of size : " + listGenericTypes.size());
				}

				final ClassOrInterfaceType genericType = (ClassOrInterfaceType)listGenericTypes.get(0);
				final Class<?> genericReferencedClass = XSDSparkSchemaService.determineFullClass(inputClass, genericType.getNameWithScope());
				if(enumsToSourcePaths.containsKey(genericReferencedClass) == true)
				{
					//Enum array - make this all strings.
					DataType targetType = DataTypes.createArrayType(DataTypes.StringType);
					memberPaths.add(new MemberPath(inputClass,
												   ordinal,
												   Collections.singletonList(variableName),
												   targetType,
												   referencedClass,
												   xmlAnnotationName,
												   genericReferencedClass));
				}
				else if(topLevelClassDeclarations.containsKey(genericReferencedClass) == true)
				{
					//Ok, we need to create an array out of these attributes and types....
					final MemberPaths genericMemberPath = getMemberPath(genericReferencedClass);
					final StructType sparkStructType = genericMemberPath.getStructType();
					final ArrayType targetType = DataTypes.createArrayType(sparkStructType);
					memberPaths.add(new MemberPath(inputClass,
												   ordinal,
												   Collections.singletonList(variableName),
												   targetType,
												   referencedClass,
												   xmlAnnotationName,
												   genericReferencedClass));
				}
				else if(genericReferencedClass.equals(List.class))
				{
					throw new RuntimeException("No, you cannot have list of lists - " + variableName + " - InputClass : " + inputClass + " - the type argument list is of size : " + listGenericTypes.size());
				}
				else
				{
					DataType targetTypeElements = XSDSparkSchemaService.determineSparkDataTypeFromLeafType(genericReferencedClass);
					final ArrayType targetType = DataTypes.createArrayType(targetTypeElements);
					memberPaths.add(new MemberPath(inputClass,
												   ordinal,
												   Collections.singletonList(variableName),
												   targetType,
												   referencedClass,
												   xmlAnnotationName,
												   genericReferencedClass));
				}
			}
			else
			{
				//We are referencing a primitive type.
				DataType targetType = XSDSparkSchemaService.determineSparkDataTypeFromLeafType(referencedClass);
				memberPaths.add(new MemberPath(inputClass,
											   ordinal,
											   Collections.singletonList(variableName),
											   targetType,
											   referencedClass,
											   xmlAnnotationName));
			}

			ordinal++;
		}

		MemberPaths paths = new MemberPaths(inputClass);
		paths.addMemberPaths(memberPaths);

		return paths;
	}

	private String extractXmlAnnotationName(String variableName, FieldDeclaration field)
	{
		Optional<AnnotationExpr> fieldAnnotation = field.getAnnotationByClass(XmlElement.class);
		if(fieldAnnotation.isPresent() == true)
		{
			final AnnotationExpr annotExpr = fieldAnnotation.get();
			List<Node> childNodes = annotExpr.getChildNodes();
			for(Node n : childNodes)
			{
				if(n instanceof MemberValuePair)
				{
					MemberValuePair mvp = (MemberValuePair)n;
					if(mvp.getNameAsString().equals("name"))
					{
						String rootValue = mvp.getValue().toString();
						return rootValue.replace("\"", "");
					}
				}
			}
		}

		logger.warn("No XML Field Declaration was found for : " + field);
		return variableName;
	}

	public Path getSourceInputDir()
	{
		return sourceInputDir;
	}

	public List<Path> getAllJavaSources()
	{
		return Collections.unmodifiableList(allJavaSources);
	}

	public Map<Class<?>, Path> getTopLevelClassesToSourcePaths()
	{
		return Collections.unmodifiableMap(topLevelClassesToSourcePaths);
	}

	public Map<Class<?>, Path> getEnumsToSourcePaths()
	{
		return Collections.unmodifiableMap(enumsToSourcePaths);
	}

	private static List<Path> getAllJavaSources(final Path sourceDir)
	{
		List< Path > allJava = new CopyOnWriteArrayList<>();
		try
		{
			Files.walkFileTree(sourceDir, new SimpleFileVisitor<Path>()
			{
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
				{
					if (file.getFileName().toString().endsWith(".java"))
					{
						allJava.add( file );
					}
					return FileVisitResult.CONTINUE;
				}
			});

		} catch( Exception x )
		{
			throw new RuntimeException( x );
		}
		return Collections.unmodifiableList(allJava);
	}


}




