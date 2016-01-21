package solutions.siren.join.action.terms.collector;

import com.carrotsearch.hppc.LongHashSet;

public class LongTermsSet extends TermsSet<Long> {

  private final LongHashSet set;

  public LongTermsSet(int expectedElements) {
    this.set = new LongHashSet(expectedElements);
  }

  @Override
  public void add(Long term) {
    this.set.add(term);
  }

  @Override
  protected void addAll(TermsSet<Long> terms) {
    if (!(terms instanceof LongTermsSet)) {
      throw new UnsupportedOperationException("Invalid type: LongTermSet expected.");
    }
    this.set.addAll(((LongTermsSet) terms).set);
  }

  @Override
  public int size() {
    return this.set.size();
  }

}
