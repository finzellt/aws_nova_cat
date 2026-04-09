'use client';

/**
 * CatalogTable
 *
 * Sortable, filterable, paginated catalog table built on TanStack Table v8.
 * Column specification: ADR-012 §Catalog Table Column Specification
 * Data schema:          ADR-014 §Catalog Artifact (catalog.json)
 * Visual design:        ADR-012 §Tables, §Iconography, §Interactive and Focus States
 *
 * Design decisions encoded here:
 * - Default sort: spectra_count descending (ADR-012)
 * - Pagination: 25 rows/page (ADR-012)
 * - Client-side search: primary_name + aliases (ADR-012)
 * - Light curve column: post-MVP slot, always renders "—" (ADR-012)
 * - photometry_count === 0 renders "—" (ADR-012)
 * - Name cell: up to 2 aliases on a second italic line (ADR-012)
 * - No vertical column dividers (ADR-012)
 * - Link target: /nova/<primary_name> (ADR-011)
 */

import React, { useState, useMemo, useCallback } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  getFilteredRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
  type PaginationState,
  type FilterFn,
  type Column,
  type Row,
} from '@tanstack/react-table';
import Link from 'next/link';
import {
  ChevronUp,
  ChevronDown,
  ChevronsUpDown,
  ChevronLeft,
  ChevronRight,
  Search,
} from 'lucide-react';
import type { NovaSummary } from '@/types/catalog';
import { getArtifactUrl } from '@/lib/dataClient';

// ── Constants ──────────────────────────────────────────────────────────────

const PAGE_SIZE = 25;

/**
 * Column IDs that should be right-aligned (numerical / coordinate cells).
 * Used both in <th> and <td> to guarantee alignment consistency.
 */
const RIGHT_ALIGNED_COLUMNS = new Set([
  'ra_dec',
  'discovery_date',
  'spectra_count',
  'spectral_visits',
  'photometry_count',
  'references_count',
  'light_curve',
]);

// ── Custom global filter ───────────────────────────────────────────────────

/**
 * Matches a row if the search query appears in the primary name or any alias.
 * Case-insensitive substring match — no fuzzy logic, intentional for precision.
 */
const novaGlobalFilter: FilterFn<NovaSummary> = (
  row: Row<NovaSummary>,
  _columnId: string,
  filterValue: string,
): boolean => {
  const query = String(filterValue).toLowerCase().trim();
  if (!query) return true;
  const { primary_name, aliases } = row.original;
  return (
    primary_name.toLowerCase().includes(query) ||
    aliases.some((alias) => alias.toLowerCase().includes(query))
  );
};

// Tells TanStack to remove the filter when the value is empty/blank.
novaGlobalFilter.autoRemove = (val: unknown) =>
  !val || !String(val).trim();

// ── Sub-components ─────────────────────────────────────────────────────────

/** Sort direction indicator rendered inside sortable column headers. */
function SortIndicator({
  column,
}: {
  column: Column<NovaSummary>;
}): React.ReactElement | null {
  const sorted = column.getIsSorted();
  if (sorted === 'asc') {
    return <ChevronUp size={13} aria-hidden="true" />;
  }
  if (sorted === 'desc') {
    return <ChevronDown size={13} aria-hidden="true" />;
  }
  // Inactive sort: show a muted double-chevron to signal the column is sortable.
  return (
    <ChevronsUpDown
      size={13}
      className="text-[var(--color-text-disabled)]"
      aria-hidden="true"
    />
  );
}

// ── Pagination helpers ─────────────────────────────────────────────────────

type PaginationItem = number | 'ellipsis-left' | 'ellipsis-right';

/**
 * Returns a compact page number range with ellipsis markers.
 * currentPage and return values are 1-indexed.
 *
 * Example (currentPage=6, totalPages=12, delta=2):
 *   [1, 'ellipsis-left', 4, 5, 6, 7, 8, 'ellipsis-right', 12]
 */
function getPaginationRange(
  currentPage: number,
  totalPages: number,
  delta = 2,
): PaginationItem[] {
  if (totalPages <= 1) return [1];

  const items: PaginationItem[] = [];
  const left = Math.max(2, currentPage - delta);
  const right = Math.min(totalPages - 1, currentPage + delta);

  items.push(1);
  if (left > 2) items.push('ellipsis-left');
  for (let i = left; i <= right; i++) items.push(i);
  if (right < totalPages - 1) items.push('ellipsis-right');
  if (totalPages > 1) items.push(totalPages);

  return items;
}

// ── Main component ─────────────────────────────────────────────────────────

export interface CatalogTableProps {
  /**
   * Array of nova summary records from catalog.json (ADR-014).
   * The caller is responsible for passing the full array; filtering/sorting
   * is handled client-side within this component.
   */
  novae: NovaSummary[];

  /**
   * Active release ID from resolveRelease(). Used to construct sparkline
   * artifact URLs. Falls back to "local" (dev-mode paths) when omitted.
   */
  releaseId?: string;

  /**
   * When true, renders a condensed preview: no search bar, no pagination,
   * all passed rows visible. Used for the homepage preview table (ADR-011).
   * The caller is responsible for slicing `novae` to the desired row count.
   */
  preview?: boolean;
  autoFocusSearch?: boolean;
}

export function CatalogTable({
  novae,
  releaseId = 'local',
  preview = false,
  autoFocusSearch = false,
}: CatalogTableProps): React.ReactElement {
  const [globalFilter, setGlobalFilter] = useState('');
  const [sorting, setSorting] = useState<SortingState>([
    { id: 'spectra_count', desc: true },
  ]);
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    // In preview mode, disable pagination by making the page large enough to
    // hold all rows the caller passes in.
    pageSize: preview ? Math.max(novae.length, 1) : PAGE_SIZE,
  });

  // ── Column definitions ───────────────────────────────────────────────────

  const columns = useMemo<ColumnDef<NovaSummary>[]>(
    () => [
      // ── Name ──────────────────────────────────────────────────────────
      {
        id: 'primary_name',
        accessorKey: 'primary_name',
        header: 'Name',
        enableSorting: true,
        cell: ({ row }) => {
          const { primary_name, aliases } = row.original;
          // ADR-012: up to 2 aliases on a second line; extras visible on nova page.
          const displayAliases = aliases.slice(0, 2);
          return (
            <div className="flex flex-col py-1" style={{ gap: '2px' }}>
              <Link
                href={`/nova/${primary_name.replace(/\s+/g, '-')}`}
                className={[
                  'font-medium text-[var(--color-interactive)]',
                  'no-underline hover:underline',
                  'rounded-sm',
                  'focus-visible:outline-none focus-visible:ring-2',
                  'focus-visible:ring-[var(--color-focus-ring)] focus-visible:ring-offset-2',
                ].join(' ')}
              >
                {primary_name}
              </Link>
              {displayAliases.length > 0 && (
                <span
                  className="text-xs italic text-[var(--color-text-tertiary)] leading-tight"
                >
                  {displayAliases.join(', ')}
                </span>
              )}
            </div>
          );
        },
      },

      // ── RA / Dec ───────────────────────────────────────────────────────
      {
        id: 'ra_dec',
        header: 'RA / Dec',
        enableSorting: false,
        // accessorFn not needed — cell renders directly from row.original
        cell: ({ row }) => {
          const { ra, dec } = row.original;
          return (
            <div className="flex flex-col items-end font-mono text-sm text-[var(--color-text-primary)] leading-snug">
              <span>{ra}</span>
              <span>{dec}</span>
            </div>
          );
        },
      },

      // ── Discovery date (displayed as year) ─────────────────────────────
      // Schema v1.1: discovery_date is "YYYY-MM-DD" | null. The column
      // header stays "Year" because only the year portion is displayed.
      // Sorting uses the full date string (lexicographic on ISO dates is
      // chronologically correct). Null values sort to the end.
      {
        accessorKey: 'discovery_date',
        header: 'Year',
        enableSorting: true,
        sortUndefined: 'last',
        cell: ({ getValue }) => {
          const date = getValue<string | null>();
          if (!date) {
            return (
              <span className="text-[var(--color-text-disabled)]">—</span>
            );
          }
          // Extract the four-digit year from "YYYY-MM-DD" format.
          const year = date.substring(0, 4);
          return (
            <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
              {year}
            </span>
          );
        },
      },

      // ── Spectra count (default sort key) ───────────────────────────────
      {
        accessorKey: 'spectra_count',
        header: 'Spectra',
        enableSorting: true,
        cell: ({ getValue }) => (
          <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
            {getValue<number>()}
          </span>
        ),
      },

      // ── Spectral visits (distinct observation nights) ──────────────────
      {
        accessorKey: 'spectral_visits',
        header: 'Visits',
        enableSorting: true,
        cell: ({ getValue }) => {
          const count = getValue<number>();
          return (
            <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
              {count === 0 ? '—' : count}
            </span>
          );
        },
      },

      // ── Photometry count ───────────────────────────────────────────────
      {
        accessorKey: 'photometry_count',
        header: 'Photometry',
        enableSorting: true,
        cell: ({ getValue }) => {
          const count = getValue<number>();
          // ADR-012: render em-dash when count is 0 (no data).
          return (
            <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
              {count === 0 ? '—' : count}
            </span>
          );
        },
      },

      // ── References count ───────────────────────────────────────────────
      {
        accessorKey: 'references_count',
        header: 'References',
        enableSorting: true,
        cell: ({ getValue }) => (
          <span className="font-mono tabular-nums text-[var(--color-text-primary)]">
            {getValue<number>()}
          </span>
        ),
      },

      // ── Light curve sparkline ────────────────────────────────────────
      // Renders a sparkline SVG when available; em-dash placeholder otherwise.
      // Path segment logic mirrors NovaPage: dev uses primary_name, prod uses nova_id.
      {
        id: 'light_curve',
        header: 'Light Curve',
        enableSorting: false,
        cell: ({ row }) => {
          if (!row.original.has_sparkline) {
            return (
              <span
                className="text-[var(--color-text-disabled)]"
                aria-label="Light curve not yet available"
              >
                —
              </span>
            );
          }
          const pathSegment =
            releaseId === 'local'
              ? encodeURIComponent(row.original.primary_name)
              : row.original.nova_id;
          const src = getArtifactUrl(
            releaseId,
            `nova/${pathSegment}/sparkline.svg`,
          );
          return (
            <img
              src={src}
              width={90}
              height={55}
              alt={`Light curve sparkline for ${row.original.primary_name}`}
              className="block"
            />
          );
        },
      },
    ],
    [releaseId],
  );

  // ── Table instance ───────────────────────────────────────────────────────

  const table = useReactTable<NovaSummary>({
    data: novae,
    columns,

    // Use nova_id for stable row identity across re-renders and sort changes.
    getRowId: (row) => row.nova_id,

    state: { globalFilter, sorting, pagination },

    // Reset to page 0 whenever sorting changes so the user sees the top of
    // the re-sorted list, not a mid-list page.
    onSortingChange: (updater) => {
      setSorting(typeof updater === 'function' ? updater(sorting) : updater);
      setPagination((prev) => ({ ...prev, pageIndex: 0 }));
    },

    onPaginationChange: setPagination,
    onGlobalFilterChange: setGlobalFilter,
    globalFilterFn: novaGlobalFilter,

    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  });

  // ── Derived state ────────────────────────────────────────────────────────

  const currentPage = table.getState().pagination.pageIndex + 1; // 1-indexed
  const pageCount = table.getPageCount();
  const filteredTotal = table.getFilteredRowModel().rows.length;
  const paginationRange = getPaginationRange(currentPage, pageCount);

  // ── Handlers ─────────────────────────────────────────────────────────────

  const handleSearchChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      setGlobalFilter(e.target.value);
      // Reset to first page whenever the search query changes.
      setPagination((prev) => ({ ...prev, pageIndex: 0 }));
    },
    [],
  );

  // ── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col gap-4">

      {/* ── Search bar ───────────────────────────────────────────────── */}
      {!preview && (
        <div className="flex items-center gap-4">
          <div role="search" className="relative w-full max-w-xs">
            <Search
              size={15}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--color-text-tertiary)] pointer-events-none"
              aria-hidden="true"
            />
            <input
              type="search"
              autoFocus={autoFocusSearch}
              value={globalFilter}
              onChange={handleSearchChange}
              placeholder="Search by name or alias…"
              aria-label="Search novae by name or alias"
              className={[
                'w-full pl-8 pr-3 py-2 text-sm rounded-md',
                'border border-[var(--color-border-default)]',
                'bg-[var(--color-surface-primary)]',
                'text-[var(--color-text-primary)]',
                'placeholder:text-[var(--color-text-disabled)]',
                'focus-visible:outline-none focus-visible:ring-2',
                'focus-visible:ring-[var(--color-focus-ring)] focus-visible:ring-offset-2',
                'transition-colors',
              ].join(' ')}
            />
          </div>

          {/* Live result count — only visible while actively filtering */}
          {globalFilter.trim() && (
            <p
              aria-live="polite"
              aria-atomic="true"
              className="text-sm text-[var(--color-text-tertiary)] shrink-0"
            >
              {filteredTotal === 0
                ? 'No results'
                : `${filteredTotal} ${filteredTotal === 1 ? 'nova' : 'novae'}`}
            </p>
          )}
        </div>
      )}

      {/* ── Table ────────────────────────────────────────────────────── */}
      <div className="overflow-x-auto rounded-md border border-[var(--color-border-subtle)]">
        <table
          className="w-full border-collapse text-sm"
          aria-label="Nova catalog"
        >
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr
                key={headerGroup.id}
                className={[
                  'bg-[var(--color-surface-secondary)]',
                  'border-b border-[var(--color-border-subtle)]',
                ].join(' ')}
              >
                {headerGroup.headers.map((header) => {
                  const canSort = header.column.getCanSort();
                  const sorted = header.column.getIsSorted();
                  const isActiveSortColumn = sorted !== false;
                  const isRightAligned = RIGHT_ALIGNED_COLUMNS.has(header.id);

                  return (
                    <th
                      key={header.id}
                      scope="col"
                      aria-sort={
                        !canSort
                          ? undefined
                          : sorted === 'asc'
                          ? 'ascending'
                          : sorted === 'desc'
                          ? 'descending'
                          : 'none'
                      }
                      tabIndex={canSort ? 0 : undefined}
                      onClick={
                        canSort
                          ? header.column.getToggleSortingHandler()
                          : undefined
                      }
                      onKeyDown={
                        canSort
                          ? (e) => {
                              if (e.key === 'Enter' || e.key === ' ') {
                                e.preventDefault();
                                header.column.toggleSorting();
                              }
                            }
                          : undefined
                      }
                      className={[
                        'px-3 py-2 text-xs font-semibold whitespace-nowrap select-none',
                        isRightAligned ? 'text-right' : 'text-left',
                        // Active sort column uses primary text; others use secondary.
                        isActiveSortColumn
                          ? 'text-[var(--color-text-primary)]'
                          : 'text-[var(--color-text-secondary)]',
                        canSort
                          ? [
                              'cursor-pointer',
                              'hover:text-[var(--color-text-primary)]',
                              'focus-visible:outline-none focus-visible:ring-2',
                              'focus-visible:ring-inset focus-visible:ring-[var(--color-focus-ring)]',
                              'transition-colors',
                            ].join(' ')
                          : 'cursor-default',
                      ].join(' ')}
                    >
                      {/* Sort icon is inline with the header label */}
                      <span
                        className={[
                          'inline-flex items-center gap-1',
                          isRightAligned ? 'flex-row-reverse' : '',
                        ].join(' ')}
                      >
                        {flexRender(
                          header.column.columnDef.header,
                          header.getContext(),
                        )}
                        {canSort && (
                          <SortIndicator column={header.column} />
                        )}
                      </span>
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>

          <tbody>
            {table.getRowModel().rows.length === 0 ? (
              // ── Empty state ──────────────────────────────────────────
              <tr>
                <td
                  colSpan={columns.length}
                  className="py-16 text-center text-sm text-[var(--color-text-tertiary)]"
                >
                  {globalFilter
                    ? 'No novae match your search.'
                    : 'No novae in the catalog yet.'}
                </td>
              </tr>
            ) : (
              // ── Data rows ────────────────────────────────────────────
              table.getRowModel().rows.map((row, rowIndex) => (
                <tr
                  key={row.id}
                  className={[
                    'border-b border-[var(--color-border-subtle)]',
                    'transition-colors',
                    // Row striping: even rows use surface-primary, odd use surface-secondary.
                    // ADR-012: "alternating --color-surface-primary / --color-surface-secondary"
                    rowIndex % 2 === 0
                      ? 'bg-[var(--color-surface-primary)]'
                      : 'bg-[var(--color-surface-secondary)]',
                    'hover:bg-[var(--color-interactive-subtle)]',
                  ].join(' ')}
                >
                  {row.getVisibleCells().map((cell) => {
                    const isRightAligned = RIGHT_ALIGNED_COLUMNS.has(
                      cell.column.id,
                    );
                    return (
                      <td
                        key={cell.id}
                        className={[
                          // h-12 = 48px minimum row height per ADR-012.
                          // Table cells behave as min-height in practice: the
                          // name cell with an alias line will expand naturally.
                          'px-3 h-12 align-middle',
                          isRightAligned ? 'text-right' : 'text-left',
                        ].join(' ')}
                      >
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext(),
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* ── Pagination ───────────────────────────────────────────────── */}
      {/* Hidden in preview mode and when the result fits on one page. */}
      {!preview && pageCount > 1 && (
        <nav
          aria-label="Catalog pagination"
          className="flex items-center justify-end gap-1"
        >
          {/* Previous page */}
          <button
            type="button"
            aria-label="Previous page"
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.previousPage()}
            className={[
              'inline-flex items-center justify-center w-9 h-9 rounded-md',
              'text-sm font-medium transition-colors',
              'focus-visible:outline-none focus-visible:ring-2',
              'focus-visible:ring-[var(--color-focus-ring)] focus-visible:ring-offset-2',
              // Ghost style for Prev/Next (ADR-012: icon buttons, no border)
              'text-[var(--color-text-secondary)]',
              'hover:text-[var(--color-interactive)] hover:enabled:bg-transparent',
              'disabled:cursor-not-allowed disabled:text-[var(--color-text-disabled)]',
            ].join(' ')}
          >
            <ChevronLeft size={16} />
          </button>

          {/* Page number buttons */}
          {paginationRange.map((item, i) => {
            if (
              item === 'ellipsis-left' ||
              item === 'ellipsis-right'
            ) {
              return (
                <span
                  key={`${item}-${i}`}
                  className="inline-flex items-center justify-center w-9 h-9 text-sm text-[var(--color-text-tertiary)] select-none"
                  aria-hidden="true"
                >
                  …
                </span>
              );
            }

            const isCurrentPage = item === currentPage;
            return (
              <button
                key={item}
                type="button"
                aria-label={`Page ${item}`}
                aria-current={isCurrentPage ? 'page' : undefined}
                onClick={() =>
                  table.setPageIndex((item as number) - 1)
                }
                className={[
                  'inline-flex items-center justify-center w-9 h-9 rounded-md',
                  'text-sm font-medium transition-colors',
                  'focus-visible:outline-none focus-visible:ring-2',
                  'focus-visible:ring-[var(--color-focus-ring)] focus-visible:ring-offset-2',
                  isCurrentPage
                    ? // Secondary style for current page (ADR-012)
                      [
                        'border border-[var(--color-border-default)]',
                        'text-[var(--color-text-primary)] bg-transparent',
                        'hover:bg-[var(--color-surface-secondary)]',
                      ].join(' ')
                    : // Ghost style for other pages (ADR-012)
                      [
                        'border border-transparent bg-transparent',
                        'text-[var(--color-text-secondary)]',
                        'hover:text-[var(--color-interactive)]',
                      ].join(' '),
                ].join(' ')}
              >
                {item}
              </button>
            );
          })}

          {/* Next page */}
          <button
            type="button"
            aria-label="Next page"
            disabled={!table.getCanNextPage()}
            onClick={() => table.nextPage()}
            className={[
              'inline-flex items-center justify-center w-9 h-9 rounded-md',
              'text-sm font-medium transition-colors',
              'focus-visible:outline-none focus-visible:ring-2',
              'focus-visible:ring-[var(--color-focus-ring)] focus-visible:ring-offset-2',
              'text-[var(--color-text-secondary)]',
              'hover:text-[var(--color-interactive)] hover:enabled:bg-transparent',
              'disabled:cursor-not-allowed disabled:text-[var(--color-text-disabled)]',
            ].join(' ')}
          >
            <ChevronRight size={16} />
          </button>
        </nav>
      )}
    </div>
  );
}
