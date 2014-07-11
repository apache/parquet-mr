package parquet.filter2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import parquet.ColumnPath;
import parquet.filter2.FilterPredicateOperators.And;
import parquet.filter2.FilterPredicateOperators.BooleanColumn;
import parquet.filter2.FilterPredicateOperators.Column;
import parquet.filter2.FilterPredicateOperators.DoubleColumn;
import parquet.filter2.FilterPredicateOperators.Eq;
import parquet.filter2.FilterPredicateOperators.Gt;
import parquet.filter2.FilterPredicateOperators.IntColumn;
import parquet.filter2.FilterPredicateOperators.Not;
import parquet.filter2.FilterPredicateOperators.Or;
import parquet.filter2.FilterPredicateOperators.UserDefined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.filter2.Filter.and;
import static parquet.filter2.Filter.booleanColumn;
import static parquet.filter2.Filter.doubleColumn;
import static parquet.filter2.Filter.eq;
import static parquet.filter2.Filter.gt;
import static parquet.filter2.Filter.intColumn;
import static parquet.filter2.Filter.not;
import static parquet.filter2.Filter.notEq;
import static parquet.filter2.Filter.or;
import static parquet.filter2.Filter.userDefined;
import static parquet.filter2.FilterPredicateOperators.NotEq;

public class TestFilterConstructionMethods {

  private static final IntColumn intColumn = intColumn("a.b.c");
  private static final DoubleColumn doubleColumn = doubleColumn("x.y.z");

  private static final FilterPredicate predicate =
      and(not(or(eq(intColumn, 7), notEq(intColumn, 17))), gt(doubleColumn, 100.0));

  @Test
  public void testFilterPredicateCreation() {
    FilterPredicate outerAnd = predicate;

    assertTrue(outerAnd instanceof And);

    FilterPredicate not = ((And) outerAnd).getLeft();
    FilterPredicate gt = ((And) outerAnd).getRight();
    assertTrue(not instanceof Not);

    FilterPredicate or = ((Not) not).getPredicate();
    assertTrue(or instanceof Or);

    FilterPredicate leftEq = ((Or) or).getLeft();
    FilterPredicate rightNotEq = ((Or) or).getRight();
    assertTrue(leftEq instanceof Eq);
    assertTrue(rightNotEq instanceof NotEq);
    assertEquals(7, ((Eq) leftEq).getValue());
    assertEquals(17, ((NotEq) rightNotEq).getValue());
    assertEquals(ColumnPath.get("a", "b", "c"), ((Eq) leftEq).getColumn().getColumnPath());
    assertEquals(ColumnPath.get("a", "b", "c"), ((NotEq) rightNotEq).getColumn().getColumnPath());

    assertTrue(gt instanceof Gt);
    assertEquals(100.0, ((Gt) gt).getValue());
    assertEquals(ColumnPath.get("x", "y", "z"), ((Gt) gt).getColumn().getColumnPath());
  }

  @Test
  public void testToString() {
    assertEquals("and(not(or(eq(a.b.c, 7), noteq(a.b.c, 17))), gt(x.y.z, 100.0))",
        predicate.toString());
  }

  @Test
  public void testUdp() {
    FilterPredicate predicate = or(eq(doubleColumn, 12.0), userDefined(intColumn, DummyUdp.class));
    assertTrue(predicate instanceof Or);
    FilterPredicate ud = ((Or) predicate).getRight();
    assertTrue(ud instanceof UserDefined);
    assertEquals(DummyUdp.class, ((UserDefined) ud).getUserDefinedPredicateClass());
    assertTrue(((UserDefined) ud).getUserDefinedPredicate() instanceof DummyUdp);
  }

  @Test
  public void testSerializable() throws Exception {
    FilterPredicate p = and(userDefined(intColumn, DummyUdp.class), predicate);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(p);
    oos.close();

    ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    FilterPredicate read = (FilterPredicate) is.readObject();
    assertEquals(p, read);
  }
}
