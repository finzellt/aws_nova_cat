/**
 * Type declaration for react-plotly.js.
 *
 * react-plotly.js doesn't ship its own TypeScript types, so we declare
 * the module here. This tells TypeScript: "this module exists, and its
 * default export is a React component that accepts Plotly props."
 *
 * The props interface covers only the fields we actually use. Plotly's
 * full type surface is enormous; declaring everything would be wasted
 * effort for the MVP.
 */
declare module 'react-plotly.js' {
  import type { ComponentType } from 'react';

  interface PlotProps {
    data: Array<Record<string, unknown>>;
    layout?: Record<string, unknown>;
    config?: Record<string, unknown>;
    style?: React.CSSProperties;
    useResizeHandler?: boolean;
    onInitialized?: (figure: unknown, graphDiv: HTMLElement) => void;
    onUpdate?: (figure: unknown, graphDiv: HTMLElement) => void;
  }

  const Plot: ComponentType<PlotProps>;
  export default Plot;
}
