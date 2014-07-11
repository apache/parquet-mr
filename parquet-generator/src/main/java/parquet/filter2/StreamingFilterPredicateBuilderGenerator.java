package parquet.filter2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StreamingFilterPredicateBuilderGenerator {
  private FileWriter writer;

  public StreamingFilterPredicateBuilderGenerator(File file) throws IOException {
    this.writer = new FileWriter(file);
  }

  private static class TypeInfo {
    public final String className;
    public final String primitiveName;
    public final boolean useComparable;
    public final boolean supportsInequality;

    private TypeInfo(String className, String primitiveName, boolean useComparable, boolean supportsInequality) {
      this.className = className;
      this.primitiveName = primitiveName;
      this.useComparable = useComparable;
      this.supportsInequality = supportsInequality;
    }
  }

  private static final TypeInfo[] TYPES = new TypeInfo[]{
    new TypeInfo("Integer", "int", false, true),
    new TypeInfo("Long", "long", false, true),
    new TypeInfo("Boolean", "boolean", false, false),
    new TypeInfo("Float", "float", false, true),
    new TypeInfo("Double", "double", false, true),
    new TypeInfo("Binary", "Binary", true, true),
  };

  public void run() throws IOException {
    add(
        "package parquet.filter2;\n" +
        "\n" +
        "import java.util.ArrayList;\n" +
        "import java.util.HashMap;\n" +
        "import java.util.List;\n" +
        "import java.util.Map;\n" +
        "\n" +
        "import parquet.ColumnPath;\n" +
        "import parquet.filter2.FilterPredicate.Visitor;\n" +
        "import parquet.filter2.FilterPredicateOperators.And;\n" +
        "import parquet.filter2.FilterPredicateOperators.Eq;\n" +
        "import parquet.filter2.FilterPredicateOperators.Gt;\n" +
        "import parquet.filter2.FilterPredicateOperators.GtEq;\n" +
        "import parquet.filter2.FilterPredicateOperators.LogicalNotUserDefined;\n" +
        "import parquet.filter2.FilterPredicateOperators.Lt;\n" +
        "import parquet.filter2.FilterPredicateOperators.LtEq;\n" +
        "import parquet.filter2.FilterPredicateOperators.Not;\n" +
        "import parquet.filter2.FilterPredicateOperators.NotEq;\n" +
        "import parquet.filter2.FilterPredicateOperators.Or;\n" +
        "import parquet.filter2.FilterPredicateOperators.UserDefined;\n" +
        "import parquet.filter2.StreamingFilterPredicate.Atom;\n" +
        "import parquet.io.api.Binary;\n" +
        "\n");

    add("/**\n" +
        " * This class is auto-generated by parquet.filter2.StreamingFilterPredicateBuilderGenerator\n" +
        " * Do not manually edit!\n" +
        " *\n" +
        " * Constructs a {@link parquet.filter2.StreamingFilterPredicate} from a {@link parquet.filter2.FilterPredicate}\n" +
        " * This is how records are filtered during record assembly. This file is generated in order to avoid autoboxing.\n" +
        " *\n" +
        " * Note: the supplied predicate must not contain any instances of the not() operator as this is not\n" +
        " * supported by this filter.\n" +
        " *\n" +
        " * the supplied predicate should first be run through {@link parquet.filter2.CollapseLogicalNots} to rewrite it\n" +
        " * in a form that doesn't make use of the not() operator.\n" +
        " *\n" +
        " * the supplied predicate should also have already been run through\n" +
        " * {@link parquet.filter2.FilterPredicateTypeValidator}\n" +
        " * to make sure it is compatible with the schema of this file.\n" +
        "\n" +
        " * TODO(alexlevenson): user defined functions still autobox however\n" +
        " */\n");


     add("public class StreamingFilterPredicateBuilder implements Visitor<StreamingFilterPredicate> {\n" +
        "\n" +
        "  private final Map<ColumnPath, List<Atom>> atomsByColumn = new HashMap<ColumnPath, List<Atom>>();\n" +
        "\n" +
        "  private StreamingFilterPredicateBuilder() { }\n" +
        "\n" +
        "  public StreamingFilterPredicate build(FilterPredicate pred) {\n" +
        "    return pred.accept(new StreamingFilterPredicateBuilder());\n" +
        "  }\n" +
        "\n" +
        "  private void addAtom(ColumnPath columnPath, Atom atom) {\n" +
        "    List<Atom> atoms = atomsByColumn.get(columnPath);\n" +
        "    if (atoms == null) {\n" +
        "      atoms = new ArrayList<Atom>();\n" +
        "      atomsByColumn.put(columnPath, atoms);\n" +
        "    }\n" +
        "    atoms.add(atom);\n" +
        "  }\n\n" +
        "  public Map<ColumnPath, List<Atom>> getAtomsByColumn() {\n" +
            "    return atomsByColumn;\n" +
            "  }\n\n"
    );

    addVisitBegin("Eq");
    for (TypeInfo info : TYPES) {
      addEqNotEqCase(info, true);
    }
    addVisitEnd();

    addVisitBegin("NotEq");
    for (TypeInfo info : TYPES) {
      addEqNotEqCase(info, false);
    }
    addVisitEnd();

    addVisitBegin("Lt");
    for (TypeInfo info : TYPES) {
      addInequalityCase(info, "<");
    }
    addVisitEnd();

    addVisitBegin("LtEq");
    for (TypeInfo info : TYPES) {
      addInequalityCase(info, "<=");
    }
    addVisitEnd();

    addVisitBegin("Gt");
    for (TypeInfo info : TYPES) {
      addInequalityCase(info, ">");
    }
    addVisitEnd();

    addVisitBegin("GtEq");
    for (TypeInfo info : TYPES) {
      addInequalityCase(info, ">=");
    }
    addVisitEnd();

    add("  @Override\n" +
        "  public StreamingFilterPredicate visit(And and) {\n" +
        "    return new parquet.filter2.StreamingFilterPredicate.And(and.getLeft().accept(this), and.getRight().accept(this));\n" +
        "  }\n" +
        "\n" +
        "  @Override\n" +
        "  public StreamingFilterPredicate visit(Or or) {\n" +
        "    return new parquet.filter2.StreamingFilterPredicate.Or(or.getLeft().accept(this), or.getRight().accept(this));\n" +
        "  }\n" +
        "\n" +
        "  @Override\n" +
        "  public StreamingFilterPredicate visit(Not not) {\n" +
        "    throw new IllegalArgumentException(\n" +
        "        \"This predicate contains a not! Did you forget to run this predicate through CollapseLogicalNots? \" + not);\n" +
        "  }\n\n");

    add("  @Override\n" +
        "  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> StreamingFilterPredicate visit(UserDefined<T, U> pred) {\n");
    addUdpBegin();
    for (TypeInfo info : TYPES) {
      addUdpCase(info, false);
    }
    addVisitEnd();

    add("  @Override\n" +
        "  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> StreamingFilterPredicate visit(LogicalNotUserDefined<T, U> notPred) {\n" +
        "    UserDefined<T, U> pred = notPred.getUserDefined();\n");
    addUdpBegin();
    for (TypeInfo info : TYPES) {
      addUdpCase(info, true);
    }
    addVisitEnd();

    add("}\n");
    writer.close();
  }

  private void addVisitBegin(String inVar) {
    add("  @Override\n" +
        "  public <T extends Comparable<T>> StreamingFilterPredicate visit(" + inVar + "<T> pred) {\n" +
        "    ColumnPath columnPath = pred.getColumn().getColumnPath();\n" +
        "    Class<T> clazz = pred.getColumn().getColumnType();\n" +
        "\n" +
        "    Atom atom = null;\n\n");
  }

  private void addVisitEnd() {
    add("    if (atom == null) {\n" +
        "      throw new IllegalArgumentException(\"Encountered unknown type \" + clazz);\n" +
        "    }\n" +
        "\n" +
        "    addAtom(columnPath, atom);\n" +
        "    return atom;\n" +
        "  }\n\n");
  }

  private void addEqNotEqCase(TypeInfo info, boolean isEq) {
    add("    if (clazz.equals(" + info.className + ".class)) {\n" +
        "      if (pred.getValue() == null) {\n" +
        "        atom = new Atom() {\n" +
        "          @Override\n" +
        "          public void updateNull() {\n" +
        "            setResult(" + isEq + ");\n" +
        "          }\n" +
        "\n" +
        "          @Override\n" +
        "          public void update(" + info.primitiveName + " value) {\n" +
        "            setResult(" + !isEq + ");\n" +
        "          }\n" +
        "        };\n" +
        "      } else {\n" +
        "        final " + info.primitiveName + " target = (" + info.className + ") (Object) pred.getValue();\n" +
        "\n" +
        "        atom = new Atom() {\n" +
        "          @Override\n" +
        "          public void updateNull() {\n" +
        "            setResult(" + !isEq +");\n" +
        "          }\n" +
        "\n" +
        "          @Override\n" +
        "          public void update(" + info.primitiveName + " value) {\n");

    if (info.useComparable) {
      add("            setResult(" + compareEquality("value", "target", isEq) + ");\n");
    } else {
      add("            setResult(" + (isEq ? "value == target" : "value != target" )  + ");\n");
    }

    add("          }\n" +
        "        };\n" +
        "      }\n" +
        "    }\n\n");
  }

  private void addInequalityCase(TypeInfo info, String op) {
    if (!info.supportsInequality) {
      add("    if (clazz.equals(" + info.className + ".class)) {\n");
      add("      throw new IllegalArgumentException(\"Operator " + op + " not supported for " + info.className + "\");\n");
      add("    }\n\n");
      return;
    }

    add("    if (clazz.equals(" + info.className + ".class)) {\n" +
        "      final " + info.primitiveName + " target = (" + info.className + ") (Object) pred.getValue();\n" +
        "\n" +
        "      atom = new Atom() {\n" +
        "        @Override\n" +
        "        public void updateNull() {\n" +
        "          setResult(false);\n" +
        "        }\n" +
        "\n" +
        "        @Override\n" +
        "        public void update(" + info.primitiveName + " value) {\n");

    if (info.useComparable) {
      add("          setResult(value.compareTo(target) " + op + " 0);\n");
    } else {
      add("          setResult(value " + op + " target);\n");
    }
    add("        }\n" +
        "      };\n" +
        "    }\n\n");
  }

  private void addUdpBegin() {
    add("    ColumnPath columnPath = pred.getColumn().getColumnPath();\n" +
        "    Class<T> clazz = pred.getColumn().getColumnType();\n" +
        "\n" +
        "    Atom atom = null;\n" +
        "\n" +
        "    final U udp = pred.getUserDefinedPredicate();\n" +
        "\n");
  }

  private void addUdpCase(TypeInfo info, boolean invert) {
    add("    if (clazz.equals(" + info.className + ".class)) {\n" +
        "      atom = new Atom() {\n" +
        "        @Override\n" +
        "        public void updateNull() {\n" +
        "          setResult(" + (invert ? "!" : "") + "udp.keep(null));\n" +
        "        }\n" +
        "\n" +
        "        @SuppressWarnings(\"unchecked\")\n" +
        "        @Override\n" +
        "        public void update(" + info.primitiveName + " value) {\n" +
        "          setResult(" + (invert ? "!" : "") + "udp.keep((T) (Object) value));\n" +
        "        }\n" +
        "      };\n" +
        "    }\n\n");
  }

  private String compareEquality(String var, String target, boolean eq) {
    return var + ".compareTo(" + target + ")" + (eq ? " == 0 " : " != 0");
  }

  private void add(String s) {
    try {
      writer.write(s);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws IOException {
    new StreamingFilterPredicateBuilderGenerator(new File(args[0])).run();
  }
}
