// Themed date-time picker. The browser-native datetime-local popup can't be
// styled, so we replace it with our own calendar + time UI. Presentation only:
// the chosen value is written raw (YYYY-MM-DDTHH:mm, local) to
// `input.dataset.value` — exactly what `new Date(...)` already consumes upstream
// — and shown human-friendly in `input.value`. A "change" event fires on apply
// and clear so callers can react. Safe to drop in: no value-format change.

import { zonedWallToUtc, utcToZonedWall, tzShort } from "../tz.js";

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

// A naive Date holding wall-clock fields (used only for calendar/time display).
const wallFromZoned = ({ y, mo, d, h, mi }) => new Date(y, mo, d, h, mi);
const WD = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

const pad = (n) => String(n).padStart(2, "0");
const sameDay = (a, b) =>
  a.getFullYear() === b.getFullYear() &&
  a.getMonth() === b.getMonth() &&
  a.getDate() === b.getDate();

function toRaw(d) {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function toDisplay(d) {
  return `${pad(d.getDate())} ${MONTHS[d.getMonth()].slice(0, 3)} ${d.getFullYear()}, ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function parseRaw(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Turn a readonly text input into a themed date-time picker.
 * @param {HTMLInputElement} input
 */
export function attachDatePicker(input) {
  let popup = null;
  let view = parseRaw(input.dataset.value) || new Date();
  // The in-progress selection while the popup is open (committed on "Done").
  let pending = parseRaw(input.dataset.value);

  const isOpen = () => popup !== null;

  function commit(date) {
    if (date) {
      // `date` holds the wall-clock the user picked (in the selected zone). Store
      // the true UTC instant so the query path sends the right time regardless of
      // the display zone; show the entered wall-clock in the field.
      const utc = zonedWallToUtc(
        date.getFullYear(), date.getMonth(), date.getDate(),
        date.getHours(), date.getMinutes(),
      );
      input.dataset.value = utc.toISOString();
      input.value = toDisplay(date);
    } else {
      input.dataset.value = "";
      input.value = "";
    }
    input.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function timeFromInputs() {
    if (!popup) return { h: 0, m: 0 };
    const h = Math.min(23, Math.max(0, parseInt(popup.querySelector(".dp-h").value, 10) || 0));
    const m = Math.min(59, Math.max(0, parseInt(popup.querySelector(".dp-m").value, 10) || 0));
    return { h, m };
  }

  function dayCell(d, muted, today) {
    const cls = ["dp-day"];
    if (muted) cls.push("dp-muted");
    if (pending && sameDay(d, pending)) cls.push("dp-sel");
    if (sameDay(d, today)) cls.push("dp-today-c");
    return `<button type="button" class="${cls.join(" ")}" data-d="${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}">${d.getDate()}</button>`;
  }

  function render() {
    const y = view.getFullYear();
    const m = view.getMonth();
    const startWd = new Date(y, m, 1).getDay();
    const daysInMonth = new Date(y, m + 1, 0).getDate();
    const today = new Date();
    let cells = "";
    for (let i = 0; i < startWd; i++) {
      cells += dayCell(new Date(y, m, 1 - (startWd - i)), true, today);
    }
    for (let day = 1; day <= daysInMonth; day++) {
      cells += dayCell(new Date(y, m, day), false, today);
    }
    const filled = startWd + daysInMonth;
    for (let i = 1; i <= 42 - filled; i++) {
      cells += dayCell(new Date(y, m + 1, i), true, today);
    }
    const t = pending || new Date();
    popup.innerHTML = `
      <div class="dp-head">
        <button type="button" class="dp-nav" data-nav="-1" aria-label="Previous month">‹</button>
        <span class="dp-title">${MONTHS[m]} ${y}</span>
        <button type="button" class="dp-nav" data-nav="1" aria-label="Next month">›</button>
      </div>
      <div class="dp-grid dp-wd">${WD.map((w) => `<span class="dp-wdc">${w}</span>`).join("")}</div>
      <div class="dp-grid dp-days">${cells}</div>
      <div class="dp-time">
        <span class="dp-time-lbl">Time <span class="dp-tz">· ${tzShort()}</span></span>
        <input class="dp-h" type="number" min="0" max="23" value="${pad(t.getHours())}" aria-label="Hour" />
        <span class="dp-colon">:</span>
        <input class="dp-m" type="number" min="0" max="59" value="${pad(t.getMinutes())}" aria-label="Minute" />
      </div>
      <div class="dp-foot">
        <button type="button" class="dp-clear">Clear</button>
        <div class="dp-foot-r">
          <button type="button" class="dp-today">Today</button>
          <button type="button" class="dp-done">Done</button>
        </div>
      </div>`;

    popup.querySelectorAll(".dp-nav").forEach((b) => {
      b.onclick = () => { view = new Date(y, m + Number(b.dataset.nav), 1); render(); };
    });
    popup.querySelectorAll(".dp-day").forEach((b) => {
      b.onclick = () => {
        const [yy, mm, dd] = b.dataset.d.split("-").map(Number);
        const { h, m: mi } = timeFromInputs();
        pending = new Date(yy, mm - 1, dd, h, mi);
        view = new Date(yy, mm - 1, 1);
        render();
      };
    });
    const syncTime = () => {
      const { h, m: mi } = timeFromInputs();
      pending = pending || new Date();
      pending.setHours(h, mi, 0, 0);
    };
    popup.querySelector(".dp-h").onchange = syncTime;
    popup.querySelector(".dp-m").onchange = syncTime;
    popup.querySelector(".dp-clear").onclick = () => { pending = null; commit(null); close(); };
    popup.querySelector(".dp-today").onclick = () => {
      pending = new Date(); pending.setSeconds(0, 0); view = new Date(); render();
    };
    popup.querySelector(".dp-done").onclick = () => { commit(pending); close(); };
  }

  function place() {
    const r = input.getBoundingClientRect();
    popup.style.top = `${r.bottom + 6}px`;
    let left = r.left;
    const pw = popup.offsetWidth;
    if (left + pw > window.innerWidth - 8) left = Math.max(8, window.innerWidth - pw - 8);
    popup.style.left = `${left}px`;
  }

  function open() {
    if (isOpen()) return;
    // Stored value is a UTC instant — show it as the wall-clock in the selected zone.
    const utc = parseRaw(input.dataset.value);
    pending = utc ? wallFromZoned(utcToZonedWall(utc)) : null;
    view = pending ? new Date(pending) : new Date();
    popup = document.createElement("div");
    popup.className = "dp-pop";
    document.body.appendChild(popup);
    render();
    place();
    // defer so the opening click doesn't immediately close it
    setTimeout(() => document.addEventListener("mousedown", onOutside), 0);
    window.addEventListener("resize", place);
    window.addEventListener("scroll", place, true);
    document.addEventListener("keydown", onKey);
  }

  function close() {
    if (!isOpen()) return;
    document.removeEventListener("mousedown", onOutside);
    window.removeEventListener("resize", place);
    window.removeEventListener("scroll", place, true);
    document.removeEventListener("keydown", onKey);
    popup.remove();
    popup = null;
  }

  function onOutside(e) {
    if (popup && !popup.contains(e.target) && e.target !== input) close();
  }
  function onKey(e) {
    if (e.key === "Escape") { close(); input.focus(); }
  }

  input.readOnly = true;
  input.addEventListener("click", () => (isOpen() ? close() : open()));
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " " || e.key === "ArrowDown") { e.preventDefault(); open(); }
  });
}
