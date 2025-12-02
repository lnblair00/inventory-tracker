import re
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import altair as alt
import pandas as pd
import sqlite3
import streamlit as st

st.set_page_config(page_title="Company Inventory", layout="wide")

# -----------------------------------------------------------------------------
# Config

DB_FILENAME = Path(__file__).parent / "inventory.db"

LOGIN_USER = "lblair@mercuryrising.ie"
LOGIN_NAME = "Lauryn Blair"
LOGIN_PASSWORD = "whatever"  # prototype only


# -----------------------------------------------------------------------------
# Helpers


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_str(x) -> str:
    return "" if pd.isna(x) else str(x).strip()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILENAME)
    conn.row_factory = sqlite3.Row
    return conn


def initialise_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # Master data
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            crs_mrs TEXT,
            name TEXT,
            product_code TEXT,
            supplier TEXT,
            unit TEXT,
            min_stock REAL DEFAULT 0
        )
        """
    )

    # Append-only log
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_utc TEXT,
            person_name TEXT,
            person_email TEXT,
            type TEXT,
            crs_mrs TEXT,
            name TEXT,
            product_code TEXT,
            supplier TEXT,
            unit TEXT,
            qty_change REAL,
            reason TEXT,
            reason_other TEXT,
            rec_number TEXT,
            comments TEXT
        )
        """
    )

    # Optional: choices (keeps dropdown lists tidy)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lookups (
            key TEXT,
            value TEXT
        )
        """
    )

    conn.commit()


def seed_if_empty(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS n FROM products")
    n = int(cur.fetchone()[0])
    if n > 0:
        return

    # Very small seed so the app runs immediately.
    cur.execute(
        """
        INSERT INTO products (type, crs_mrs, name, product_code, supplier, unit, min_stock)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("Manufacturing", "CRS-0001", "Example Item", "EX-0001", "Example Supplier", "piece", 5),
    )

    # Seed lookup values
    lookup_seed = [
        ("supplier", "Example Supplier"),
        ("reason", "Receipt"),
        ("reason", "Issued to Production"),
        ("reason", "QC testing"),
        ("reason", "Write-off"),
        ("reason", "Other"),
    ]
    cur.executemany("INSERT INTO lookups (key, value) VALUES (?, ?)", lookup_seed)

    conn.commit()


def df_from_query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def get_lookup_values(conn: sqlite3.Connection, key: str) -> list[str]:
    df = df_from_query(conn, "SELECT value FROM lookups WHERE key = ? ORDER BY value", (key,))
    values = [safe_str(v) for v in df["value"].tolist() if safe_str(v)]
    return values


def compute_current_stock(conn: sqlite3.Connection) -> pd.Series:
    df = df_from_query(
        conn,
        """
        SELECT product_code, COALESCE(SUM(qty_change), 0) AS current_stock
        FROM stock_movements
        GROUP BY product_code
        """,
    )
    if df.empty:
        return pd.Series(dtype=float)
    return pd.Series(df.current_stock.values, index=df.product_code.astype(str))


def export_excel(products: pd.DataFrame, movements: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        products.to_excel(writer, sheet_name="Products", index=False)
        movements.to_excel(writer, sheet_name="StockMovements", index=False)
    return output.getvalue()


def require_login() -> None:
    if "authed" not in st.session_state:
        st.session_state.authed = False
    if "user_email" not in st.session_state:
        st.session_state.user_email = ""
    if "user_name" not in st.session_state:
        st.session_state.user_name = ""

    if st.session_state.authed:
        return

    st.title("Inventory")
    st.subheader("Sign in (prototype)")
    with st.form("login"):
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in")

    if submitted:
        if email.strip().lower() == LOGIN_USER and password == LOGIN_PASSWORD:
            st.session_state.authed = True
            st.session_state.user_email = LOGIN_USER
            st.session_state.user_name = LOGIN_NAME
            st.rerun()
        else:
            st.error("Invalid credentials")

    st.stop()


# -----------------------------------------------------------------------------
# App start

require_login()

conn = connect_db()
initialise_db(conn)
seed_if_empty(conn)

# Load data
products = df_from_query(
    conn,
    """
    SELECT id, type, crs_mrs, name, product_code, supplier, unit, min_stock
    FROM products
    """,
)

movements = df_from_query(
    conn,
    """
    SELECT id, timestamp_utc, person_name, person_email, type, crs_mrs, name, product_code, supplier,
           unit, qty_change, reason, reason_other, rec_number, comments
    FROM stock_movements
    ORDER BY timestamp_utc DESC
    """,
)

current_stock = compute_current_stock(conn)
products = products.copy()
products["current_stock"] = products["product_code"].astype(str).map(current_stock).fillna(0.0)
products["low_stock"] = products["current_stock"] < products["min_stock"].fillna(0.0)

st.title("Inventory")

# Tabs: 3 only
tab_dashboard, tab_visuals, tab_logging = st.tabs(["Dashboard", "Visuals", "Logging"])


# -----------------------------------------------------------------------------
# Dashboard

with tab_dashboard:
    st.subheader("Overview")

    # Page-specific filters ONLY
    f1, f2, f3 = st.columns([1.1, 2.2, 1.2])
    with f1:
        types = ["All"] + sorted([t for t in products["type"].dropna().astype(str).unique().tolist() if t])
        type_sel = st.selectbox("Type", options=types, key="dash_type")
    with f2:
        search = st.text_input("MRS/CRS & Name", value="", key="dash_search").strip().lower()
    with f3:
        suppliers = ["All"] + sorted([s for s in products["supplier"].dropna().astype(str).unique().tolist() if s])
        supplier_sel = st.selectbox("Supplier", options=suppliers, key="dash_supplier")

    view = products.copy()
    if type_sel != "All":
        view = view[view["type"].astype(str) == str(type_sel)]
    if supplier_sel != "All":
        view = view[view["supplier"].astype(str) == str(supplier_sel)]
    if search:
        view = view[
            view["name"].astype(str).str.lower().str.contains(search)
            | view["crs_mrs"].astype(str).str.lower().str.contains(search)
            | view["product_code"].astype(str).str.lower().str.contains(search)
        ]

    m1, m2 = st.columns(2)
    m1.metric("Products (filtered)", int(view.shape[0]))
    m2.metric("Low stock (filtered)", int(view["low_stock"].sum()))

    st.dataframe(
        view.sort_values(["low_stock", "name"], ascending=[False, True])[[
            "type", "crs_mrs", "name", "product_code", "supplier", "unit", "current_stock", "min_stock", "low_stock"
        ]],
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------------------------------------------------------
# Visuals (kept basic for now)

with tab_visuals:
    st.subheader("Visuals")

    f1, f2, f3 = st.columns([1.1, 2.2, 1.2])
    with f1:
        types = ["All"] + sorted([t for t in products["type"].dropna().astype(str).unique().tolist() if t])
        type_sel = st.selectbox("Type", options=types, key="viz_type")
    with f2:
        search = st.text_input("MRS/CRS & Name", value="", key="viz_search").strip().lower()
    with f3:
        suppliers = ["All"] + sorted([s for s in products["supplier"].dropna().astype(str).unique().tolist() if s])
        supplier_sel = st.selectbox("Supplier", options=suppliers, key="viz_supplier")

    view = products.copy()
    if type_sel != "All":
        view = view[view["type"].astype(str) == str(type_sel)]
    if supplier_sel != "All":
        view = view[view["supplier"].astype(str) == str(supplier_sel)]
    if search:
        view = view[
            view["name"].astype(str).str.lower().str.contains(search)
            | view["crs_mrs"].astype(str).str.lower().str.contains(search)
            | view["product_code"].astype(str).str.lower().str.contains(search)
        ]

    if view.empty:
        st.info("No products match your filters.")
    else:
        # Prettier selector label
        view = view.copy()
        view["label"] = view.apply(lambda r: f"{safe_str(r['crs_mrs'])} | {safe_str(r['product_code'])} — {safe_str(r['name'])}", axis=1)
        label_to_code = dict(zip(view["label"], view["product_code"].astype(str)))

        selected = st.selectbox("Select product", options=["(Select a product)"] + sorted(view["label"].tolist()))

        if selected == "(Select a product)":
            st.info("Select a product to see usage over time.")
        else:
            code = label_to_code[selected]
            prod_row = products.loc[products["product_code"].astype(str) == str(code)].iloc[0].to_dict()

            # Stock over time from movements
            mv = movements[movements["product_code"].astype(str) == str(code)].copy()
            if mv.empty:
                st.warning("No movements recorded for this product yet.")
            else:
                mv["timestamp_utc"] = pd.to_datetime(mv["timestamp_utc"], errors="coerce", utc=True)
                mv = mv.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc")

                ts = mv[["timestamp_utc", "qty_change"]].rename(columns={"timestamp_utc": "TimestampUTC"}).copy()
                ts["stock_level"] = ts["qty_change"].cumsum()

                min_stock = float(prod_row.get("min_stock", 0.0) or 0.0)

                chart_df = ts.set_index("TimestampUTC")
                chart_df["min_stock"] = min_stock

                st.line_chart(chart_df[["stock_level", "min_stock"]])


# -----------------------------------------------------------------------------
# Logging (movement entry + basic admin editing in separate section)

with tab_logging:
    st.subheader("Logging")
    st.caption(f"Signed in as {st.session_state.user_name} ({st.session_state.user_email})")

    mode = st.radio(
        "Action",
        options=[
            "1. Change stock of item currently in system",
            "2. Add new item to system (from PO)",
            "3. Add new item to system (not from PO, R&D ONLY)",
        ],
        index=0,
    )

    # Shared filters on this tab
    f1, f2, f3 = st.columns([1.1, 2.2, 1.2])
    with f1:
        types = ["All"] + sorted([t for t in products["type"].dropna().astype(str).unique().tolist() if t])
        type_sel = st.selectbox("Type", options=types, key="log_type")
    with f2:
        search = st.text_input("MRS/CRS & Name", value="", key="log_search").strip().lower()
    with f3:
        suppliers = ["All"] + sorted([s for s in products["supplier"].dropna().astype(str).unique().tolist() if s])
        supplier_sel = st.selectbox("Supplier", options=suppliers, key="log_supplier")

    view = products.copy()
    if type_sel != "All":
        view = view[view["type"].astype(str) == str(type_sel)]
    if supplier_sel != "All":
        view = view[view["supplier"].astype(str) == str(supplier_sel)]
    if search:
        view = view[
            view["name"].astype(str).str.lower().str.contains(search)
            | view["crs_mrs"].astype(str).str.lower().str.contains(search)
            | view["product_code"].astype(str).str.lower().str.contains(search)
        ]

    if mode.startswith("1"):
        st.markdown("### Change stock")

        if view.empty:
            st.info("No products match your filters.")
        else:
            view = view.copy()
            view["label"] = view.apply(lambda r: f"{safe_str(r['crs_mrs'])} | {safe_str(r['product_code'])} — {safe_str(r['name'])}", axis=1)
            label_to_code = dict(zip(view["label"], view["product_code"].astype(str)))

            selected = st.selectbox("Select item", options=["(Select an item)"] + sorted(view["label"].tolist()))

            if selected == "(Select an item)":
                st.info("Select an item to log a movement.")
            else:
                code = label_to_code[selected]
                prod_row = products.loc[products["product_code"].astype(str) == str(code)].iloc[0].to_dict()

                # Non-editable fields
                unit = safe_str(prod_row.get("unit", ""))
                name = safe_str(prod_row.get("name", ""))
                crs_mrs = safe_str(prod_row.get("crs_mrs", ""))
                ptype = safe_str(prod_row.get("type", ""))

                st.write({
                    "Type": ptype,
                    "CRS/MRS": crs_mrs,
                    "Name": name,
                    "Product code": code,
                    "Unit": unit,
                    "Current stock": float(prod_row.get("current_stock", 0.0) or 0.0),
                    "Min stock": float(prod_row.get("min_stock", 0.0) or 0.0),
                })

                supplier_options = get_lookup_values(conn, "supplier")
                if not supplier_options:
                    supplier_options = sorted([s for s in products["supplier"].dropna().astype(str).unique().tolist() if s])

                reason_options = get_lookup_values(conn, "reason")
                if not reason_options:
                    reason_options = ["Receipt", "Issued to Production", "QC testing", "Write-off", "Other"]

                # Existing rec numbers for OUT dropdown
                recs_df = df_from_query(conn, "SELECT DISTINCT rec_number FROM stock_movements WHERE product_code = ? AND rec_number IS NOT NULL AND rec_number <> '' ORDER BY rec_number", (code,))
                rec_numbers = [safe_str(x) for x in recs_df["rec_number"].tolist() if safe_str(x)]

                with st.form("movement_form"):
                    c1, c2, c3 = st.columns([1.0, 1.0, 1.2])
                    with c1:
                        direction = st.selectbox("Direction", options=["OUT", "IN"], index=0)
                    with c2:
                        qty = st.number_input("Quantity", min_value=0.0, value=1.0, step=1.0)
                    with c3:
                        supplier = st.selectbox("Supplier", options=supplier_options, index=0)

                    st.text_input("Unit (fixed)", value=unit, disabled=True)

                    r1, r2 = st.columns([1.0, 2.0])
                    with r1:
                        reason = st.selectbox("Reason", options=reason_options)
                    with r2:
                        reason_other = st.text_input("Other reason (required if Reason = Other)")

                    if direction == "OUT":
                        rec_number = st.selectbox("Rec number (OUT)", options=(rec_numbers if rec_numbers else [""]))
                    else:
                        rec_number = st.text_input("Rec number (IN)")

                    comments = st.text_area("Comments", value="")
                    submitted = st.form_submit_button("Submit")

                if submitted:
                    if qty <= 0:
                        st.error("Quantity must be greater than 0")
                        st.stop()

                    if reason == "Other" and not safe_str(reason_other):
                        st.error("Other reason is required when Reason is Other")
                        st.stop()

                    qty_change = float(qty) if direction == "IN" else -float(qty)

                    # Insert movement row
                    conn.execute(
                        """
                        INSERT INTO stock_movements (
                            timestamp_utc, person_name, person_email,
                            type, crs_mrs, name, product_code, supplier, unit,
                            qty_change, reason, reason_other, rec_number, comments
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now_utc_iso(),
                            st.session_state.user_name,
                            st.session_state.user_email,
                            ptype,
                            crs_mrs,
                            name,
                            code,
                            supplier,
                            unit,
                            qty_change,
                            reason,
                            safe_str(reason_other) if reason == "Other" else "",
                            safe_str(rec_number),
                            safe_str(comments),
                        ),
                    )
                    conn.commit()

                    st.success("Movement logged.")
                    st.rerun()

    else:
        st.info("We will implement Add-from-PO and R&D-only add flows next.")

    st.divider()
    st.subheader("Admin: Edit products")
    st.caption("This edits the master product list. Stock is always calculated from the movement log.")

    # Only allow the known admin in this prototype
    is_admin = st.session_state.user_email.strip().lower() == LOGIN_USER

    if not is_admin:
        st.info("Admin editor is not available for your account.")
    else:
        editable = products[["id", "type", "crs_mrs", "name", "product_code", "supplier", "unit", "min_stock"]].copy()

        st.warning(
            "Avoid changing product_code for existing items unless you understand the impact. "
            "Movements are linked to product_code. If you change it, historical movements will no longer match."
        )

        edited = st.data_editor(
            editable,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "type": st.column_config.TextColumn("Type"),
                "crs_mrs": st.column_config.TextColumn("CRS/MRS"),
                "name": st.column_config.TextColumn("Name"),
                "product_code": st.column_config.TextColumn("ProductCode"),
                "supplier": st.column_config.TextColumn("Supplier"),
                "unit": st.column_config.TextColumn("Unit"),
                "min_stock": st.column_config.NumberColumn("Min stock", min_value=0.0, step=1.0),
            },
        )

        c1, c2 = st.columns([1, 3])
        with c1:
            commit_products = st.button("Commit product changes")

        if commit_products:
            # Basic validation
            if edited["product_code"].astype(str).str.strip().eq("").any():
                st.error("ProductCode cannot be blank.")
                st.stop()

            # Duplicate ProductCode check
            pc = edited["product_code"].astype(str).str.strip()
            if pc.duplicated().any():
                st.error("Duplicate ProductCode found. Each ProductCode should be unique.")
                st.stop()

            # Determine row-level changes
            before = editable.set_index("id")
            after = edited.copy()
            after["id"] = pd.to_numeric(after["id"], errors="coerce")

            # Rows with existing IDs
            existing = after.dropna(subset=["id"]).copy()
            existing["id"] = existing["id"].astype(int)

            # New rows (id missing)
            new_rows = after[after["id"].isna()].copy()

            # Update existing rows
            for _, row in existing.iterrows():
                rid = int(row["id"])
                if rid not in before.index:
                    continue

                # Build update payload
                conn.execute(
                    """
                    UPDATE products SET
                        type = ?, crs_mrs = ?, name = ?, product_code = ?, supplier = ?, unit = ?, min_stock = ?
                    WHERE id = ?
                    """,
                    (
                        safe_str(row["type"]),
                        safe_str(row["crs_mrs"]),
                        safe_str(row["name"]),
                        safe_str(row["product_code"]),
                        safe_str(row["supplier"]),
                        safe_str(row["unit"]),
                        float(row["min_stock"]) if not pd.isna(row["min_stock"]) else 0.0,
                        rid,
                    ),
                )

            # Insert new rows
            for _, row in new_rows.iterrows():
                conn.execute(
                    """
                    INSERT INTO products (type, crs_mrs, name, product_code, supplier, unit, min_stock)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        safe_str(row.get("type", "")),
                        safe_str(row.get("crs_mrs", "")),
                        safe_str(row.get("name", "")),
                        safe_str(row.get("product_code", "")),
                        safe_str(row.get("supplier", "")),
                        safe_str(row.get("unit", "")),
                        float(row.get("min_stock", 0.0)) if not pd.isna(row.get("min_stock", 0.0)) else 0.0,
                    ),
                )

            conn.commit()
            st.success("Products updated.")
            st.rerun()

    st.divider()
    st.subheader("Exports")
    if st.button("Download inventory + movement log (Excel)"):
        # Refresh data for export
        products2 = df_from_query(conn, "SELECT id, type, crs_mrs, name, product_code, supplier, unit, min_stock FROM products")
        movements2 = df_from_query(conn, "SELECT * FROM stock_movements ORDER BY timestamp_utc DESC")
        # add computed current_stock
        current2 = compute_current_stock(conn)
        products2 = products2.copy()
        products2["current_stock"] = products2["product_code"].astype(str).map(current2).fillna(0.0)

        payload = export_excel(products2, movements2)
        st.download_button(
            "Download Excel",
            data=payload,
            file_name="inventory_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

st.caption(
    "Note: This prototype writes to a local sqlite file (inventory.db) for simplicity. "
    "If you deploy to a shared server, everyone will see the same live data."
)

#To run app: streamlit run "C:\Users\LaurynBlair\OneDrive - Mercury Rising Ltd\Projects\Production Scheduler\App.py"
