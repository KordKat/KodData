package hello1.koddata.utils;

import java.util.Objects;
import java.util.function.Function;

public class Either<F, S> {

    private final F left;
    private final S right;

    private Either(F left, S right) {
        this.left = left;
        this.right = right;
    }

    public static <F, S> Either<F, S> left(F value) {
        return new Either<>(value, null);
    }

    public static <F, S> Either<F, S> right(S value) {
        return new Either<>(null, value);
    }

    public boolean isLeft() {
        return left != null;
    }

    public boolean isRight() {
        return right != null;
    }

    public F getLeft() {
        return left;
    }

    public S getRight() {
        return right;
    }

    public <F2> Either<F2, S> mapLeft(Function<? super F, ? extends F2> mapper) {
        if (isLeft()) {
            return Either.left(mapper.apply(left));
        }
        return Either.right(right);
    }

    public <S2> Either<F, S2> mapRight(Function<? super S, ? extends S2> mapper) {
        if (isRight()) {
            return Either.right(mapper.apply(right));
        }
        return Either.left(left);
    }

    //Polymorphism
    @Override
    public String toString() {
        return isLeft() ? "Left(" + left + ")" : "Right(" + right + ")";
    }

    //Polymorphism
    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    //Polymorphism
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Either<?, ?> other)) return false;
        return Objects.equals(left, other.left)
                && Objects.equals(right, other.right);
    }
}
