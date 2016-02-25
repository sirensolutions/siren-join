package solutions.siren.join.common;

/**
 * Helper class for mathematical operations.
 */
public class Math {

  /**
   * Returns the value of the {@code long} argument;
   * throwing an exception if the value overflows an {@code int}.
   *
   * @param value the long value
   * @return the argument as an int
   * @throws ArithmeticException if the {@code argument} overflows an int
   */
  public static int toIntExact(long value) {
    if ((int)value != value) {
      throw new ArithmeticException("integer overflow");
    }
    return (int)value;
  }

}
