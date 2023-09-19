/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.defaults.internal.expressions;

import java.util.Arrays;
import java.util.Optional;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import static io.delta.kernel.defaults.internal.DefaultKernelUtils.checkArgument;
import static io.delta.kernel.defaults.internal.expressions.ExpressionUtils.compare;
import static io.delta.kernel.defaults.internal.expressions.ExpressionUtils.evalNullability;
import static io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo;

/**
 * Implementation of {@link ExpressionEvaluator} for default {@link ExpressionHandler}.
 * It takes care of validating, adding necessary implicit casts and evaluating the
 * {@link Expression} on given {@link ColumnarBatch}.
 */
public class DefaultExpressionEvaluator implements ExpressionEvaluator {
    private final Expression expression;

    /**
     * Create a {@link DefaultExpressionEvaluator} instance bound to the given expression and
     * <i>inputSchem</i>.
     *
     * @param inputSchema Input data schema
     * @param expression  Expression to evaluate.
     * @param outputType  Expected result data type.
     */
    public DefaultExpressionEvaluator(
        StructType inputSchema,
        Expression expression,
        DataType outputType) {
        ExpressionTransformResult transformResult =
            new ExpressionTransformer(inputSchema).visit(expression);
        if (!transformResult.outputType.equivalent(outputType)) {
            throw new UnsupportedOperationException(format("Can not create an expression handler " +
                "for expression `%s` returns result of type %s", expression, outputType));
        }
        this.expression = transformResult.expression;
    }

    @Override
    public ColumnVector eval(ColumnarBatch input) {
        return new ExpressionEvalVisitor(input).visit(expression);
    }

    @Override
    public void close() { /* nothing to close */ }

    /**
     * Encapsulates the result of {@link ExpressionTransformer}
     */
    private static class ExpressionTransformResult {
        public final Expression expression; // transformed expression
        public final DataType outputType; // output type of the expression

        ExpressionTransformResult(Expression expression, DataType outputType) {
            this.expression = expression;
            this.outputType = outputType;
        }
    }

    /**
     * Implementation of {@link ExpressionVisitor} to validate the given expression as follows.
     * <ul>
     *     <li>given input column is part of the input data schema</li>
     *     <li>expression inputs are of supported types. Insert cast according to the rules in
     *     {@link ImplicitCastExpression} to make the types compatible for evaluation by
     *     {@link ExpressionEvalVisitor}
     *     </li>
     * </ul>
     * <p>
     * Return type of each expression visit is a tuple of new rewritten expression and its result
     * data type.
     */
    private static class ExpressionTransformer
        extends ExpressionVisitor<ExpressionTransformResult> {
        private StructType inputDataSchema;

        ExpressionTransformer(StructType inputDataSchema) {
            this.inputDataSchema = requireNonNull(inputDataSchema, "inputDataSchema is null");
        }

        @Override
        ExpressionTransformResult visitAnd(And and) {
            Predicate left = validateIsPredicate(and, visit(and.getLeft()));
            Predicate right = validateIsPredicate(and, visit(and.getRight()));
            return new ExpressionTransformResult(new And(left, right), BooleanType.INSTANCE);
        }

        @Override
        ExpressionTransformResult visitOr(Or or) {
            Predicate left = validateIsPredicate(or, visit(or.getLeft()));
            Predicate right = validateIsPredicate(or, visit(or.getRight()));
            return new ExpressionTransformResult(new Or(left, right), BooleanType.INSTANCE);
        }

        @Override
        ExpressionTransformResult visitAlwaysTrue(AlwaysTrue alwaysTrue) {
            // nothing to validate or rewrite.
            return new ExpressionTransformResult(alwaysTrue, BooleanType.INSTANCE);
        }

        @Override
        ExpressionTransformResult visitAlwaysFalse(AlwaysFalse alwaysFalse) {
            // nothing to validate or rewrite.
            return new ExpressionTransformResult(alwaysFalse, BooleanType.INSTANCE);
        }

        @Override
        ExpressionTransformResult visitComparator(Predicate predicate) {
            switch (predicate.getName()) {
                case "=":
                case ">":
                case ">=":
                case "<":
                case "<=":
                    return new ExpressionTransformResult(
                        transformBinaryComparator(predicate),
                        BooleanType.INSTANCE);
                default:
                    throw new UnsupportedOperationException(
                        "unsupported expression encountered: " + predicate);
            }
        }

        @Override
        ExpressionTransformResult visitLiteral(Literal literal) {
            // nothing to validate or rewrite
            return new ExpressionTransformResult(literal, literal.getDataType());
        }

        @Override
        ExpressionTransformResult visitColumn(Column column) {
            int ordinal = inputDataSchema.indexOf(column.getName());
            if (ordinal == -1) {
                throw new IllegalArgumentException(
                    format("Column `%s` doesn't exist in input data schema: %s",
                        column.getName(), inputDataSchema));
            }
            return new ExpressionTransformResult(column, inputDataSchema.at(ordinal).getDataType());
        }

        @Override
        ExpressionTransformResult visitCast(ImplicitCastExpression cast) {
            throw new UnsupportedOperationException("CAST expression is not expected.");
        }

        private Predicate validateIsPredicate(
            Expression baseExpression,
            ExpressionTransformResult result) {
            checkArgument(
                result.outputType instanceof BooleanType &&
                    result.expression instanceof Predicate,
                "%s: expected a predicate expression but got %s with output type %s.",
                baseExpression,
                result.expression,
                result.outputType);
            return (Predicate) result.expression;
        }

        private Expression transformBinaryComparator(Predicate predicate) {
            checkArgument(predicate.getChildren().size() == 2, "expected two inputs");
            ExpressionTransformResult leftResult = visit(predicate.getChildren().get(0));
            ExpressionTransformResult rightResult = visit(predicate.getChildren().get(1));
            Expression left = leftResult.expression;
            Expression right = rightResult.expression;
            if (!leftResult.outputType.equivalent(rightResult.outputType)) {
                if (canCastTo(leftResult.outputType, rightResult.outputType)) {
                    left = new ImplicitCastExpression(left, rightResult.outputType);
                } else if (canCastTo(rightResult.outputType, leftResult.outputType)) {
                    right = new ImplicitCastExpression(right, leftResult.outputType);
                } else {
                    String msg = format("%s: operands are of different types which are not " +
                            "comparable: left type=%s, right type=%s",
                        predicate, leftResult.outputType, rightResult.outputType);
                    throw new UnsupportedOperationException(msg);
                }
            }
            return new Predicate(predicate.getName(), Arrays.asList(left, right));
        }
    }

    /**
     * Implementation of {@link ExpressionVisitor} to evaluate expression on a
     * {@link ColumnarBatch}.
     */
    private static class ExpressionEvalVisitor extends ExpressionVisitor<ColumnVector> {
        private final ColumnarBatch input;

        ExpressionEvalVisitor(ColumnarBatch input) {
            this.input = input;
        }

        @Override
        ColumnVector visitAnd(And and) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(and);
            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = evalNullability(argResults.leftResult, argResults.rightResult);
            for (int rowId = 0; rowId < numRows; rowId++) {
                result[rowId] = argResults.leftResult.getBoolean(rowId) &&
                    argResults.rightResult.getBoolean(rowId);
            }
            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitOr(Or or) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(or);
            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = evalNullability(argResults.leftResult, argResults.rightResult);
            for (int rowId = 0; rowId < numRows; rowId++) {
                result[rowId] = argResults.leftResult.getBoolean(rowId) ||
                    argResults.rightResult.getBoolean(rowId);
            }
            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitAlwaysTrue(AlwaysTrue alwaysTrue) {
            return new DefaultConstantVector(BooleanType.INSTANCE, input.getSize(), true);
        }

        @Override
        ColumnVector visitAlwaysFalse(AlwaysFalse alwaysFalse) {
            return new DefaultConstantVector(BooleanType.INSTANCE, input.getSize(), false);
        }

        @Override
        ColumnVector visitComparator(Predicate predicate) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(predicate);

            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = evalNullability(argResults.leftResult, argResults.rightResult);
            int[] compareResult = compare(argResults.leftResult, argResults.rightResult);
            switch (predicate.getName()) {
                case "=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] == 0;
                    }
                    break;
                case ">":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] > 0;
                    }
                    break;
                case ">=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] >= 0;
                    }
                    break;
                case "<":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] < 0;
                    }
                    break;
                case "<=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] <= 0;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "unsupported expression encountered: " + predicate);
            }

            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitLiteral(Literal literal) {
            DataType dataType = literal.getDataType();
            if (dataType instanceof BooleanType ||
                dataType instanceof ByteType ||
                dataType instanceof ShortType ||
                dataType instanceof IntegerType ||
                dataType instanceof LongType ||
                dataType instanceof FloatType ||
                dataType instanceof DoubleType ||
                dataType instanceof StringType ||
                dataType instanceof BinaryType ||
                dataType instanceof DecimalType ||
                dataType instanceof DateType ||
                dataType instanceof TimestampType) {
                return new DefaultConstantVector(dataType, input.getSize(), literal.getValue());
            }

            throw new UnsupportedOperationException(
                "unsupported expression encountered: " + literal);
        }

        @Override
        ColumnVector visitColumn(Column column) {
            int ordinal = input.getSchema().indexOf(column.getName());
            if (ordinal == -1) {
                throw new IllegalArgumentException(
                    format("Column `%s` doesn't exist in input data schema: %s",
                        column.getName(), input.getSchema()));
            }
            return input.getColumnVector(ordinal);
        }

        @Override
        ColumnVector visitCast(ImplicitCastExpression cast) {
            ColumnVector inputResult = visit(cast.getInput());
            return cast.eval(inputResult);
        }

        /**
         * Utility method to evaluate inputs to the binary input expression. Also validates the
         * evaluated expression result {@link ColumnVector}s are of the same size.
         *
         * @param predicate
         * @return Triplet of (result vector size, left operand result, left operand result)
         */
        private PredicateChildrenEvalResult evalBinaryExpressionChildren(Predicate predicate) {
            checkArgument(predicate.getChildren().size() == 2, "expected two inputs");
            ColumnVector left = visit(predicate.getChildren().get(0));
            ColumnVector right = visit(predicate.getChildren().get(1));
            checkArgument(
                left.getSize() == right.getSize(),
                "Left and right operand returned different results: left=%d, right=d",
                left.getSize(),
                right.getSize());
            return new PredicateChildrenEvalResult(left.getSize(), left, right);
        }
    }

    /**
     * Encapsulates children expression result of binary input predicate
     */
    private static class PredicateChildrenEvalResult {
        public final int rowCount;
        public final ColumnVector leftResult;
        public final ColumnVector rightResult;

        PredicateChildrenEvalResult(
            int rowCount, ColumnVector leftResult, ColumnVector rightResult) {
            this.rowCount = rowCount;
            this.leftResult = leftResult;
            this.rightResult = rightResult;
        }
    }
}
