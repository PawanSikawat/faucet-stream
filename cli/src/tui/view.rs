//! Rendering for the live TUI. The ratatui draw call is thin; everything
//! that decides *what* to show (formatting, column selection under narrow
//! widths, status labels) is a pure function with unit tests.

use super::metrics::{RowStats, TuiModel};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

/// A column of the invocation table. Order = display order; `min_width` is
/// the terminal width at which the column is still shown (columns with the
/// highest thresholds drop first on narrow terminals).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    Row,
    Route,
    In,
    Out,
    Rate,
    Errors,
    Dlq,
    Bookmark,
    Status,
}

impl Column {
    fn header(self) -> &'static str {
        match self {
            Column::Row => "row",
            Column::Route => "source → sink",
            Column::In => "in",
            Column::Out => "out",
            Column::Rate => "rec/s",
            Column::Errors => "errors",
            Column::Dlq => "dlq",
            Column::Bookmark => "bookmark",
            Column::Status => "status",
        }
    }

    fn constraint(self) -> Constraint {
        match self {
            Column::Row => Constraint::Min(8),
            Column::Route => Constraint::Min(18),
            Column::In => Constraint::Length(10),
            Column::Out => Constraint::Length(10),
            Column::Rate => Constraint::Length(9),
            Column::Errors => Constraint::Length(7),
            Column::Dlq => Constraint::Length(6),
            Column::Bookmark => Constraint::Length(9),
            Column::Status => Constraint::Length(8),
        }
    }
}

/// Which columns fit a terminal of `width` cells. Drops the least essential
/// columns first; `row`, `out`, and `status` always survive.
pub fn visible_columns(width: u16) -> Vec<Column> {
    let mut cols = vec![Column::Row, Column::Out, Column::Status];
    if width >= 44 {
        cols.insert(2, Column::Rate);
    }
    if width >= 58 {
        cols.insert(1, Column::Route);
    }
    if width >= 70 {
        cols.insert(cols.len() - 2, Column::Errors);
    }
    if width >= 82 {
        let at = cols.iter().position(|c| *c == Column::Out).unwrap();
        cols.insert(at, Column::In);
    }
    if width >= 92 {
        let at = cols.iter().position(|c| *c == Column::Status).unwrap();
        cols.insert(at, Column::Dlq);
    }
    if width >= 104 {
        let at = cols.iter().position(|c| *c == Column::Status).unwrap();
        cols.insert(at, Column::Bookmark);
    }
    cols
}

/// `1234567` → `1,234,567`.
pub fn format_count(n: u64) -> String {
    let raw = n.to_string();
    let mut out = String::with_capacity(raw.len() + raw.len() / 3);
    for (i, c) in raw.chars().enumerate() {
        if i > 0 && (raw.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Records/second with a sensible precision ramp.
pub fn format_rate(rate: f64) -> String {
    if rate <= 0.0 {
        "-".to_string()
    } else if rate < 10.0 {
        format!("{rate:.1}")
    } else if rate < 100_000.0 {
        format_count(rate.round() as u64)
    } else {
        format!("{:.0}k", rate / 1000.0)
    }
}

/// Age of the last persisted bookmark relative to `now_unix`, or `-`.
pub fn format_bookmark_age(last_bookmark_unix: f64, now_unix: f64) -> String {
    if last_bookmark_unix <= 0.0 {
        return "-".to_string();
    }
    let secs = (now_unix - last_bookmark_unix).max(0.0) as u64;
    format_elapsed(std::time::Duration::from_secs(secs)) + " ago"
}

/// `1h02m03s` / `4m05s` / `12s`.
pub fn format_elapsed(d: std::time::Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}h{:02}m{:02}s", s / 3600, (s % 3600) / 60, s % 60)
    } else if s >= 60 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{s}s")
    }
}

/// Status label + color for a row.
pub fn row_status(row: &RowStats) -> (&'static str, Color) {
    match row.finished {
        Some(true) => ("done", Color::Green),
        Some(false) => ("failed", Color::Red),
        None if row.in_flight => ("running", Color::Cyan),
        None => ("pending", Color::DarkGray),
    }
}

fn cell_for(col: Column, id: &str, row: &RowStats, now_unix: f64) -> Cell<'static> {
    match col {
        Column::Row => Cell::from(if id.is_empty() { "-" } else { id }.to_string()),
        Column::Route => Cell::from(format!(
            "{} → {}",
            if row.source.is_empty() {
                "?"
            } else {
                &row.source
            },
            if row.sink.is_empty() { "?" } else { &row.sink },
        )),
        Column::In => Cell::from(format_count(row.records_in)),
        Column::Out => Cell::from(format_count(row.records_out)),
        Column::Rate => Cell::from(format_rate(row.rate)),
        Column::Errors => {
            let n = row.source_errors + row.sink_errors;
            let cell = Cell::from(format_count(n));
            if n > 0 {
                cell.style(Style::default().fg(Color::Red))
            } else {
                cell
            }
        }
        Column::Dlq => {
            let cell = Cell::from(format_count(row.dlq_records));
            if row.dlq_records > 0 {
                cell.style(Style::default().fg(Color::Yellow))
            } else {
                cell
            }
        }
        Column::Bookmark => Cell::from(format_bookmark_age(row.last_bookmark_unix, now_unix)),
        Column::Status => {
            let (label, color) = row_status(row);
            Cell::from(label).style(Style::default().fg(color))
        }
    }
}

/// Draw one frame: header, invocation table, log pane, footer.
pub fn draw(
    frame: &mut Frame<'_>,
    pipeline: &str,
    model: &TuiModel,
    elapsed: std::time::Duration,
    logs: &[String],
    cancelling: bool,
) {
    let area = frame.area();
    let [header_a, table_a, logs_a, footer_a] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(4),
        Constraint::Length(6),
        Constraint::Length(1),
    ])
    .areas(area);

    frame.render_widget(header(pipeline, model, elapsed, cancelling), header_a);
    render_table(frame, table_a, model);
    render_logs(frame, logs_a, logs);
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" q ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("cancel (flush at page boundary) · "),
        Span::styled("Ctrl-C ", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("cancel"),
    ]))
    .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, footer_a);
}

fn header<'a>(
    pipeline: &'a str,
    model: &TuiModel,
    elapsed: std::time::Duration,
    cancelling: bool,
) -> Paragraph<'a> {
    let mut spans = vec![
        Span::styled(
            format!(" faucet run · {pipeline} "),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "· {} · {} out · {} rec/s",
            format_elapsed(elapsed),
            format_count(model.total_out),
            format_rate(model.total_rate),
        )),
    ];
    if cancelling {
        spans.push(Span::styled(
            " · cancelling…",
            Style::default().fg(Color::Yellow),
        ));
    }
    Paragraph::new(Line::from(spans))
}

fn render_table(frame: &mut Frame<'_>, area: Rect, model: &TuiModel) {
    let cols = visible_columns(area.width);
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let header = Row::new(cols.iter().map(|c| Cell::from(c.header())))
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows = model.rows.iter().map(|(id, row)| {
        Row::new(
            cols.iter()
                .map(|c| cell_for(*c, id, row, now_unix))
                .collect::<Vec<_>>(),
        )
    });
    let widths: Vec<Constraint> = cols.iter().map(|c| c.constraint()).collect();
    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::TOP)
            .title(" invocations "),
    );
    frame.render_widget(table, area);
}

fn render_logs(frame: &mut Frame<'_>, area: Rect, logs: &[String]) {
    let visible = area.height.saturating_sub(1) as usize;
    let start = logs.len().saturating_sub(visible);
    let text: Vec<Line<'_>> = logs[start..]
        .iter()
        .map(|l| Line::from(l.as_str()))
        .collect();
    let para = Paragraph::new(text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::TOP).title(" log "));
    frame.render_widget(para, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_group_thousands() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(1_234_567), "1,234,567");
    }

    #[test]
    fn rates_ramp_precision() {
        assert_eq!(format_rate(0.0), "-");
        assert_eq!(format_rate(3.24), "3.2");
        assert_eq!(format_rate(1234.6), "1,235");
        assert_eq!(format_rate(250_000.0), "250k");
    }

    #[test]
    fn elapsed_formats() {
        use std::time::Duration;
        assert_eq!(format_elapsed(Duration::from_secs(12)), "12s");
        assert_eq!(format_elapsed(Duration::from_secs(65)), "1m05s");
        assert_eq!(format_elapsed(Duration::from_secs(3723)), "1h02m03s");
    }

    #[test]
    fn bookmark_age() {
        assert_eq!(format_bookmark_age(0.0, 100.0), "-");
        assert_eq!(format_bookmark_age(40.0, 100.0), "1m00s ago");
        // Clock skew (bookmark ahead of now) clamps to zero.
        assert_eq!(format_bookmark_age(200.0, 100.0), "0s ago");
    }

    #[test]
    fn narrow_terminals_drop_columns_but_keep_essentials() {
        for width in [10u16, 30, 44, 58, 70, 82, 92, 104, 200] {
            let cols = visible_columns(width);
            assert!(cols.contains(&Column::Row), "width {width}");
            assert!(cols.contains(&Column::Out), "width {width}");
            assert!(cols.contains(&Column::Status), "width {width}");
            // Wider never shows fewer columns.
        }
        let narrow = visible_columns(30).len();
        let wide = visible_columns(200).len();
        assert!(narrow < wide);
        assert_eq!(visible_columns(200).len(), 9, "all columns at full width");
        // Status stays last, row stays first at any width.
        for width in [30u16, 60, 90, 200] {
            let cols = visible_columns(width);
            assert_eq!(*cols.first().unwrap(), Column::Row);
            assert_eq!(*cols.last().unwrap(), Column::Status);
        }
    }

    fn buffer_text(terminal: &ratatui::Terminal<ratatui::backend::TestBackend>) -> String {
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    fn sample_model() -> TuiModel {
        let mut model = TuiModel::default();
        let mut healthy = RowStats {
            source: "rest".into(),
            sink: "jsonl".into(),
            records_in: 12_500,
            records_out: 12_400,
            rate: 830.0,
            ..Default::default()
        };
        healthy.in_flight = true;
        model.rows.insert("orders".into(), healthy);
        model.rows.insert(
            "users".into(),
            RowStats {
                source: "postgres".into(),
                sink: "spanner".into(),
                records_in: 4,
                records_out: 2,
                source_errors: 1,
                sink_errors: 2,
                dlq_records: 7,
                // Recent enough that the rendered age ("1m30s ago") fits
                // the bookmark column; an epoch-old bookmark would clip the
                // "ago" suffix this test asserts on.
                last_bookmark_unix: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock")
                    .as_secs_f64()
                    - 90.0,
                finished: Some(false),
                ..Default::default()
            },
        );
        model.total_out = 12_402;
        model.total_rate = 830.0;
        model
    }

    fn draw_at(width: u16, height: u16, model: &TuiModel, cancelling: bool) -> String {
        let backend = ratatui::backend::TestBackend::new(width, height);
        let mut terminal = ratatui::Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| {
                draw(
                    frame,
                    "demo-pipeline",
                    model,
                    std::time::Duration::from_secs(65),
                    &["log line one".to_string(), "log line two".to_string()],
                    cancelling,
                )
            })
            .expect("draw");
        buffer_text(&terminal)
    }

    #[test]
    fn draws_header_table_logs_and_footer_at_full_width() {
        let text = draw_at(120, 24, &sample_model(), false);
        // Header: pipeline name, elapsed, totals.
        assert!(text.contains("faucet run · demo-pipeline"), "{text}");
        assert!(text.contains("1m05s"), "{text}");
        assert!(text.contains("12,402 out"), "{text}");
        // Table: both rows with routes, counts, statuses.
        assert!(text.contains("source → sink"), "{text}");
        assert!(text.contains("rest → jsonl"), "{text}");
        assert!(text.contains("postgres → spanner"), "{text}");
        assert!(text.contains("12,400"), "{text}");
        assert!(text.contains("running"), "{text}");
        assert!(text.contains("failed"), "{text}");
        // Errors + DLQ + bookmark columns present at this width.
        assert!(text.contains("dlq"), "{text}");
        assert!(text.contains("bookmark"), "{text}");
        assert!(text.contains("ago"), "{text}");
        // Log pane + footer.
        assert!(text.contains("log line two"), "{text}");
        assert!(text.contains("cancel (flush at page boundary)"), "{text}");
        assert!(!text.contains("cancelling…"), "{text}");
    }

    #[test]
    fn cancelling_banner_shows_when_requested() {
        let text = draw_at(120, 24, &sample_model(), true);
        assert!(text.contains("cancelling…"), "{text}");
    }

    #[test]
    fn narrow_terminal_drops_optional_columns_but_renders() {
        let text = draw_at(40, 16, &sample_model(), false);
        // Essentials survive.
        assert!(text.contains("row"), "{text}");
        assert!(text.contains("out"), "{text}");
        assert!(text.contains("status"), "{text}");
        assert!(text.contains("running"), "{text}");
        // Route/bookmark columns are gone at 40 cells.
        assert!(!text.contains("source → sink"), "{text}");
        assert!(!text.contains("bookmark"), "{text}");
    }

    #[test]
    fn empty_model_renders_headers_and_placeholder_free_table() {
        let text = draw_at(80, 12, &TuiModel::default(), false);
        assert!(text.contains("faucet run · demo-pipeline"), "{text}");
        assert!(text.contains("invocations"), "{text}");
        assert!(text.contains("0 out"), "{text}");
    }

    #[test]
    fn anonymous_row_and_unknown_connectors_render_placeholders() {
        let mut model = TuiModel::default();
        model.rows.insert(String::new(), RowStats::default());
        let text = draw_at(120, 16, &model, false);
        // Empty row id renders as `-`, unknown connectors as `?`.
        assert!(text.contains("? → ?"), "{text}");
        assert!(text.contains("pending"), "{text}");
    }

    #[test]
    fn log_pane_shows_only_the_tail_that_fits() {
        let logs: Vec<String> = (0..40).map(|i| format!("logline-{i:02}")).collect();
        let backend = ratatui::backend::TestBackend::new(100, 20);
        let mut terminal = ratatui::Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| {
                draw(
                    frame,
                    "p",
                    &TuiModel::default(),
                    std::time::Duration::from_secs(1),
                    &logs,
                    false,
                )
            })
            .expect("draw");
        let text = buffer_text(&terminal);
        // The pane is 6 rows (1 border + 5 lines): newest lines win.
        assert!(text.contains("logline-39"), "{text}");
        assert!(!text.contains("logline-00"), "{text}");
    }

    #[test]
    fn statuses_map_from_row_state() {
        let mut row = RowStats::default();
        assert_eq!(row_status(&row).0, "pending");
        row.in_flight = true;
        assert_eq!(row_status(&row).0, "running");
        row.finished = Some(true);
        assert_eq!(row_status(&row).0, "done");
        row.finished = Some(false);
        assert_eq!(row_status(&row).0, "failed");
    }
}
