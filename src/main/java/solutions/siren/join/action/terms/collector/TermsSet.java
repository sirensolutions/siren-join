package solutions.siren.join.action.terms.collector;

/**
 * A set of terms.
 */
public abstract class TermsSet<E> {

  /**
   * A flag to indicate if the set of terms has been pruned
   */
  private boolean isPruned = false;

  public void setIsPruned(boolean isPruned) {
    this.isPruned = isPruned;
  }

  public boolean isPruned() {
    return isPruned;
  }

  public abstract void add(E term);

  protected abstract void addAll(TermsSet<E> terms);

  public abstract int size();

  public void merge(TermsSet<E> other) {
    this.addAll(other);
    this.isPruned |= other.isPruned;
  }

}
