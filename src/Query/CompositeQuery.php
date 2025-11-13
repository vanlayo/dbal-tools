<?php

declare(strict_types=1);

namespace Phpro\DbalTools\Query;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Phpro\DbalTools\Column\Column;
use Phpro\DbalTools\Expression\Comparison;
use Phpro\DbalTools\Expression\Expression;
use Phpro\DbalTools\Schema\Table;
use Psl\Str;
use function Psl\Dict\map;
use function Psl\Dict\merge;
use function Psl\invariant;
use function Psl\Vec\concat;
use function Psl\Vec\map_with_key;

/**
 * @psalm-import-type JoinInfo from Table
 */
final class CompositeQuery implements Expression
{
    /**
     * @param QueryBuilder                                                      $query
     * @param array<string, QueryBuilder>                                       $with
     * @param array<string, array{base: QueryBuilder, recursive: QueryBuilder}> $recursiveWith
     */
    public function __construct(
        private Connection $connection,
        private QueryBuilder $query,
        private array $with,
        private array $recursiveWith = [],
    ) {
    }

    public static function from(Connection $connection): self
    {
        return new self($connection, $connection->createQueryBuilder(), []);
    }

    public function mainQuery(): QueryBuilder
    {
        return $this->query;
    }

    public function moveMainQueryToSubQuery(string $name): self
    {
        return new self(
            $this->connection,
            $this->connection->createQueryBuilder(),
            [
                ...$this->with,
                $name => $this->mainQuery(),
            ]
        );
    }

    /**
     * @param non-empty-string $name
     *
     * @return QueryBuilder
     */
    public function subQuery(string $name): QueryBuilder
    {
        invariant(array_key_exists($name, $this->with), sprintf('Subquery "%s" does not exist.', $name));

        return $this->with[$name];
    }

    /**
     * @param non-empty-string $name
     */
    public function hasSubQuery(string $name): bool
    {
        return array_key_exists($name, $this->with);
    }

    /**
     * @param non-empty-string $name
     */
    public function createSubQuery(string $name): QueryBuilder
    {
        $query = $this->connection->createQueryBuilder();

        $this->addSubQuery($name, $query);

        return $query;
    }

    /**
     * @param non-empty-string $name
     */
    public function addSubQuery(string $name, QueryBuilder $queryBuilder): self
    {
        $this->with[$name] = $queryBuilder;

        return $this;
    }

    /**
     * Get both parts of a recursive subquery.
     *
     * @param non-empty-string $name
     *
     * @return array{base: QueryBuilder, recursive: QueryBuilder}
     */
    public function recursiveSubQuery(string $name): array
    {
        invariant(
            array_key_exists($name, $this->recursiveWith),
            'Recursive subquery "%s" does not exist.',
            $name
        );

        return $this->recursiveWith[$name];
    }

    /**
     * Get the base (anchor) query of a recursive subquery.
     *
     * @param non-empty-string $name
     */
    public function recursiveSubQueryBase(string $name): QueryBuilder
    {
        invariant(
            array_key_exists($name, $this->recursiveWith),
            'Recursive subquery "%s" does not exist.',
            $name
        );

        return $this->recursiveWith[$name]['base'];
    }

    /**
     * Get the recursive part of a recursive subquery.
     *
     * @param non-empty-string $name
     */
    public function recursiveSubQueryRecursive(string $name): QueryBuilder
    {
        invariant(
            array_key_exists($name, $this->recursiveWith),
            'Recursive subquery "%s" does not exist.',
            $name
        );

        return $this->recursiveWith[$name]['recursive'];
    }

    /**
     * @param non-empty-string $name
     */
    public function hasRecursiveSubQuery(string $name): bool
    {
        return array_key_exists($name, $this->recursiveWith);
    }

    /**
     * Create a new recursive subquery with empty QueryBuilders.
     *
     * @param non-empty-string $name
     *
     * @return array{base: QueryBuilder, recursive: QueryBuilder}
     */
    public function createRecursiveSubQuery(string $name): array
    {
        $baseQuery = $this->connection->createQueryBuilder();
        $recursiveQuery = $this->connection->createQueryBuilder();

        $this->addRecursiveSubQuery($name, $baseQuery, $recursiveQuery);

        return [
            'base' => $baseQuery,
            'recursive' => $recursiveQuery,
        ];
    }

    /**
     * Add a recursive subquery with both base and recursive parts.
     *
     * @param non-empty-string $name
     */
    public function addRecursiveSubQuery(
        string $name,
        QueryBuilder $baseQuery,
        QueryBuilder $recursiveQuery,
    ): self {
        $this->recursiveWith[$name] = [
            'base' => $baseQuery,
            'recursive' => $recursiveQuery,
        ];

        return $this;
    }

    /**
     * @param non-empty-string $withAlias
     * @param non-empty-string $fromAlias
     *
     * @return JoinInfo
     */
    public function joinOntoCte(
        string $withAlias,
        string $fromAlias,
        Column $column,
        ?Column $rightColumn = null,
    ): array {
        $rightColumn = $rightColumn ?? $column;

        return [
            'fromAlias' => $fromAlias,
            'join' => $withAlias,
            'alias' => $withAlias,
            'condition' => Comparison::equal(
                $column->from($fromAlias),
                $rightColumn->from($withAlias)
            )->toSQL(),
        ];
    }

    /**
     * This method can be used in situations where you have a table in which you have prefiltered matching data.
     * Conditions:
     * - Your main query searches for records in a table for record
     * - You already have a subquery that already pre-filtered a list of records that you are interested in.
     *
     * This function will:
     * - Add the subquery as a WITH statement
     * - Apply the pre-filtered set as a left join to the main query
     * - Return the joined column
     *
     * You can validate if there:
     * - is a match: $column IS NOT NULL
     * - is no match $column IS NULL
     *
     * @param non-empty-string $subQueryAlias
     * @param ?QueryBuilder    $targetQuery   can be used to specify a different join target than the main query
     */
    public function joinOnMatchingLookupTableRecords(
        string $subQueryAlias,
        QueryBuilder $subQuery,
        Column $joinColumn,
        ?QueryBuilder $targetQuery = null,
    ): Column {
        invariant(null !== $joinColumn->from, 'Table name must be set on security join column.');

        $this->addSubQuery($subQueryAlias, $subQuery);
        $targetQuery = $targetQuery ?? $this->mainQuery();
        $targetQuery->leftJoin(
            ...$this->joinOntoCte(
                $subQueryAlias,
                $joinColumn->from,
                $joinColumn
            )
        );

        return $this->cteColumn($subQueryAlias, $joinColumn->name);
    }

    /**
     * @param non-empty-string $withAlias
     * @param non-empty-string $name
     */
    public function cteColumn(string $withAlias, string $name): Column
    {
        return new Column($name, $withAlias);
    }

    public function execute(): Result
    {
        $params = $paramTypes = [];
        foreach ($this->with as $query) {
            $params = merge($params, $query->getParameters());
            $paramTypes = merge($paramTypes, $query->getParameterTypes());
        }

        return $this->connection->executeQuery(
            $this->toSQL(),
            merge($params, $this->query->getParameters()),
            merge($paramTypes, $this->query->getParameterTypes()),
        );
    }

    /** @return non-empty-string */
    public function toSQL(): string
    {
        if (!$this->with && !$this->recursiveWith) {
            /** @var non-empty-string */
            return $this->query->getSQL();
        }

        $ctes = concat(
            map_with_key(
                $this->with,
                static fn (string $alias, QueryBuilder $query): string => $alias.' AS ('.$query->getSQL().')'
            ),
            map_with_key(
                $this->recursiveWith,
                static fn (string $alias, array $queries): string => Str\format(
                    '%s AS (%s UNION ALL %s)',
                    $alias,
                    $queries['base']->getSQL(),
                    $queries['recursive']->getSQL()
                )
            )
        );

        return sprintf(
            '%s %s %s',
            $this->recursiveWith ? 'WITH RECURSIVE' : 'WITH',
            Str\join($ctes, ', '),
            $this->query->getSQL()
        );
    }

    /**
     * Immutably map over current main query.
     *
     * @param \Closure(QueryBuilder): QueryBuilder $modifier
     */
    public function map(\Closure $modifier): self
    {
        $new = clone $this;
        $new->query = $modifier($new->query);

        return $new;
    }

    /**
     * Make sure to clone internal query as well so that it won't be mutably altered after cloning.
     */
    public function __clone()
    {
        $this->query = clone $this->query;
        $this->with = map(
            $this->with,
            fn (QueryBuilder $query): QueryBuilder => clone $query
        );
    }
}
