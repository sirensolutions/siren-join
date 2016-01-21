package solutions.siren.join.action.terms.collector;

import com.carrotsearch.hppc.IntHashSet;

public class IntegerTermsSet extends TermsSet<Integer> {

  private final IntHashSet set;

  public IntegerTermsSet(int expectedElements) {
    this.set = new IntHashSet(expectedElements);
  }

  @Override
  public void add(Integer term) {
    this.set.add(term);
  }

  @Override
  protected void addAll(TermsSet<Integer> terms) {
    if (!(terms instanceof IntegerTermsSet)) {
      throw new UnsupportedOperationException("Invalid type: IntegerTermsSet expected.");
    }
    this.set.addAll(((IntegerTermsSet) terms).set);
  }

  @Override
  public int size() {
    return this.set.size();
  }

}
