import os
import sqlite3
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Registro de Serviços", page_icon="📋", layout="wide")
DB_PATH = (st.secrets.get("DB_PATH")
           or os.getenv("DB_PATH")
           or "tecnotooling.db")

class DB:
    def __init__(self, path: str):
        self.path = path
        self._init()
    def _conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c
    def _init(self):
        c = self._conn()
        cur = c.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS clientes(id INTEGER PRIMARY KEY AUTOINCREMENT,nome TEXT UNIQUE NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS servicos(id INTEGER PRIMARY KEY AUTOINCREMENT,nome TEXT UNIQUE NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS setores(id INTEGER PRIMARY KEY AUTOINCREMENT,nome TEXT UNIQUE NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS registros(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            carimbo TEXT NOT NULL,
            solicitante TEXT NOT NULL,
            cliente_id INTEGER NOT NULL,
            servico_id INTEGER NOT NULL,
            quantidade INTEGER NOT NULL,
            valor_unidade REAL NOT NULL,
            setor_id INTEGER,
            forma_pagamento TEXT,
            observacoes TEXT,
            FOREIGN KEY(cliente_id) REFERENCES clientes(id),
            FOREIGN KEY(servico_id) REFERENCES servicos(id),
            FOREIGN KEY(setor_id) REFERENCES setores(id)
        )""")
        c.commit()
        c.close()
    def all(self, table):
        c = self._conn()
        r = c.execute(f"SELECT * FROM {table} ORDER BY nome").fetchall()
        c.close()
        return [dict(x) for x in r]
    def ensure(self, table, nome):
        c = self._conn()
        try:
            c.execute(f"INSERT OR IGNORE INTO {table}(nome) VALUES(?)", (nome.strip(),))
            c.commit()
            row = c.execute(f"SELECT id FROM {table} WHERE nome=?", (nome.strip(),)).fetchone()
            return row["id"] if row else None
        finally:
            c.close()
    def insert_registro(self, dados):
        c = self._conn()
        try:
            c.execute(
                """INSERT INTO registros
                (carimbo, solicitante, cliente_id, servico_id, quantidade, valor_unidade, setor_id, forma_pagamento, observacoes)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    dados["carimbo"],
                    dados["solicitante"],
                    dados["cliente_id"],
                    dados["servico_id"],
                    dados["quantidade"],
                    dados["valor_unidade"],
                    dados["setor_id"],
                    dados["forma_pagamento"],
                    dados["observacoes"],
                ),
            )
            c.commit()
        finally:
            c.close()
    def query_registros(self, dt_ini=None, dt_fim=None, cliente_id=None, servico_id=None, setor_id=None, forma=None):
        base = """SELECT r.id, r.carimbo, r.solicitante,
                  c.nome AS cliente, s.nome AS servico, r.quantidade, r.valor_unidade,
                  (r.quantidade*r.valor_unidade) AS valor_total,
                  st.nome AS setor, r.forma_pagamento, r.observacoes, r.setor_id, r.cliente_id, r.servico_id
                  FROM registros r
                  JOIN clientes c ON c.id=r.cliente_id
                  JOIN servicos s ON s.id=r.servico_id
                  LEFT JOIN setores st ON st.id=r.setor_id
                  WHERE 1=1"""
        params = []
        if dt_ini:
            base += " AND datetime(r.carimbo)>=datetime(?)"
            params.append(dt_ini)
        if dt_fim:
            base += " AND datetime(r.carimbo)<=datetime(?)"
            params.append(dt_fim)
        if cliente_id:
            base += " AND r.cliente_id=?"
            params.append(cliente_id)
        if servico_id:
            base += " AND r.servico_id=?"
            params.append(servico_id)
        if setor_id:
            base += " AND r.setor_id=?"
            params.append(setor_id)
        if forma:
            base += " AND ifnull(r.forma_pagamento,'')=?"
            params.append(forma)
        base += " ORDER BY datetime(r.carimbo) DESC"
        c = self._conn()
        r = c.execute(base, tuple(params)).fetchall()
        c.close()
        return [dict(x) for x in r]

db = DB(DB_PATH)

if "view" not in st.session_state:
    st.session_state.view = "form"
if "solicitante" not in st.session_state:
    st.session_state.solicitante = os.getenv("USER") or os.getenv("USERNAME") or "Usuário"

col_head_l, col_head_r = st.columns([3,1])
with col_head_l:
    st.markdown("### 📋 Registro de Serviços" if st.session_state.view=="form" else "### 📈 Dashboard")
with col_head_r:
    if st.session_state.view == "form":
        if st.button("Dashboard", use_container_width=True):
            st.session_state.view = "dash"
            st.rerun()
    else:
        if st.button("Voltar", use_container_width=True):
            st.session_state.view = "form"
            st.rerun()

if st.session_state.view == "form":
    col_top_a, col_top_b = st.columns([1,1])
    with col_top_a:
        solicitante = st.text_input("Solicitante", value=st.session_state.solicitante, key="solicitante_input")
    with col_top_b:
        st.write("")
    st.markdown("---")
    col_form_a, col_form_b = st.columns([1,1])
    with col_form_a:
        clientes = db.all("clientes")
        op_clientes = ["+ Novo cliente"] + [c["nome"] for c in clientes]
        cliente_sel = st.selectbox("Cliente", op_clientes, index=1 if len(op_clientes) > 1 else 0)
        if cliente_sel == "+ Novo cliente":
            novo_cliente = st.text_input("Novo cliente")
            if novo_cliente:
                cliente_id = db.ensure("clientes", novo_cliente)
            else:
                cliente_id = None
        else:
            cliente_id = next((c["id"] for c in clientes if c["nome"] == cliente_sel), None)
        servs = db.all("servicos")
        op_servs = ["+ Novo serviço"] + [s["nome"] for s in servs]
        serv_sel = st.selectbox("Serviço", op_servs, index=1 if len(op_servs) > 1 else 0)
        if serv_sel == "+ Novo serviço":
            novo_serv = st.text_input("Novo serviço")
            if novo_serv:
                servico_id = db.ensure("servicos", novo_serv)
            else:
                servico_id = None
        else:
            servico_id = next((s["id"] for s in servs if s["nome"] == serv_sel), None)
        quant = st.number_input("Quantidade", min_value=1, value=1, step=1)
        valor_uni = st.number_input("Valor da unidade (R$)", min_value=0.0, value=0.0, step=0.01, format="%.2f")
    with col_form_b:
        setores = db.all("setores")
        op_setores = ["(Sem setor)", "+ Novo setor"] + [s["nome"] for s in setores]
        setor_sel = st.selectbox("Setor", op_setores, index=0)
        if setor_sel == "+ Novo setor":
            novo_setor = st.text_input("Novo setor")
            setor_id = db.ensure("setores", novo_setor) if novo_setor else None
        elif setor_sel == "(Sem setor)":
            setor_id = None
        else:
            setor_id = next((s["id"] for s in setores if s["nome"] == setor_sel), None)
        forma = st.selectbox("Forma de pagamento", ["", "Dinheiro", "Cartão", "PIX", "Boleto", "Transferência"])
        obs = st.text_area("Observações", height=110)
        carimbo_manual = st.toggle("Definir data/hora manual")
        if carimbo_manual:
            data_str = st.text_input("AAAA-MM-DD HH:MM:SS", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            carimbo = data_str
        else:
            carimbo = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    col_btn = st.columns([1,5])[0]
    with col_btn:
        salvar = st.button("Salvar registro", use_container_width=True)
    if salvar:
        ok = all([solicitante.strip(), cliente_id, servico_id, quant >= 1, valor_uni >= 0])
        if ok:
            db.insert_registro(
                {
                    "carimbo": carimbo,
                    "solicitante": solicitante.strip(),
                    "cliente_id": int(cliente_id),
                    "servico_id": int(servico_id),
                    "quantidade": int(quant),
                    "valor_unidade": float(valor_uni),
                    "setor_id": setor_id if setor_id else None,
                    "forma_pagamento": forma if forma else None,
                    "observacoes": obs.strip() if obs else None,
                }
            )
            st.success("Registro salvo")
            st.rerun()
        else:
            st.error("Preencha os campos obrigatórios")
    st.markdown("---")
    f_a, f_b, f_c, f_d = st.columns([1,1,1,2])
    with f_a:
        clientes = db.all("clientes")
        op_cli_f = ["Todos"] + [c["nome"] for c in clientes]
        f_cli = st.selectbox("Filtro cliente", op_cli_f)
        f_cli_id = None if f_cli == "Todos" else next((c["id"] for c in clientes if c["nome"] == f_cli), None)
    with f_b:
        servs = db.all("servicos")
        op_sv_f = ["Todos"] + [s["nome"] for s in servs]
        f_sv = st.selectbox("Filtro serviço", op_sv_f)
        f_sv_id = None if f_sv == "Todos" else next((s["id"] for s in servs if s["nome"] == f_sv), None)
    with f_c:
        dt_ini = st.text_input("Início (AAAA-MM-DD)", value="")
    with f_d:
        dt_fim = st.text_input("Fim (AAAA-MM-DD)", value="")
    dt_ini_q = f"{dt_ini.strip()} 00:00:00" if dt_ini.strip() else None
    dt_fim_q = f"{dt_fim.strip()} 23:59:59" if dt_fim.strip() else None
    dados = db.query_registros(dt_ini_q, dt_fim_q, f_cli_id, f_sv_id)
    if dados:
        df = pd.DataFrame(dados)
        df["carimbo"] = pd.to_datetime(df["carimbo"]).dt.strftime("%d/%m/%Y %H:%M")
        df["valor_unidade"] = df["valor_unidade"].map(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        df["valor_total"] = df["valor_total"].map(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        cols = ["carimbo","solicitante","cliente","servico","quantidade","valor_unidade","valor_total","setor","forma_pagamento","observacoes"]
        st.dataframe(df[cols], use_container_width=True, hide_index=True)
    else:
        st.info("Sem registros")

else:
    clientes = db.all("clientes")
    servs = db.all("servicos")
    setores = db.all("setores")
    colf1, colf2, colf3, colf4, colf5 = st.columns(5)
    with colf1:
        f_cli = st.selectbox("Cliente", ["Todos"] + [c["nome"] for c in clientes])
        f_cli_id = None if f_cli == "Todos" else next((c["id"] for c in clientes if c["nome"] == f_cli), None)
    with colf2:
        f_sv = st.selectbox("Serviço", ["Todos"] + [s["nome"] for s in servs])
        f_sv_id = None if f_sv == "Todos" else next((s["id"] for s in servs if s["nome"] == f_sv), None)
    with colf3:
        f_st = st.selectbox("Setor", ["Todos"] + [s["nome"] for s in setores])
        f_st_id = None if f_st == "Todos" else next((s["id"] for s in setores if s["nome"] == f_st), None)
    with colf4:
        f_fp = st.selectbox("Pagamento", ["Todos", "Dinheiro", "Cartão", "PIX", "Boleto", "Transferência"])
        f_fp_val = None if f_fp == "Todos" else f_fp
    with colf5:
        dt_range = st.text_input("Período AAAA-MM-DD|AAAA-MM-DD", value="")
    if dt_range and "|" in dt_range:
        ini, fim = [x.strip() for x in dt_range.split("|", 1)]
        dt_ini_q = f"{ini} 00:00:00" if ini else None
        dt_fim_q = f"{fim} 23:59:59" if fim else None
    else:
        dt_ini_q = None
        dt_fim_q = None
    dados = db.query_registros(dt_ini_q, dt_fim_q, f_cli_id, f_sv_id, f_st_id, f_fp_val)
    if not dados:
        st.info("Sem dados para os filtros")
    else:
        df = pd.DataFrame(dados)
        df["carimbo_dt"] = pd.to_datetime(df["carimbo"])
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.metric("Registros", int(len(df)))
        with kpi2:
            st.metric("Quantidade total", int(df["quantidade"].sum()))
        with kpi3:
            st.metric("Valor total", f"R$ {df['valor_total'].sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        with kpi4:
            md = df["valor_total"].mean() if len(df) else 0
            st.metric("Ticket médio", f"R$ {md:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        c1, c2 = st.columns(2)
        with c1:
            g1 = df.groupby(["setor","forma_pagamento"], dropna=False)["valor_total"].sum().reset_index()
            g1["setor"] = g1["setor"].fillna("(Sem setor)")
            g1["forma_pagamento"] = g1["forma_pagamento"].fillna("(Sem)")
            fig1 = px.bar(g1, x="setor", y="valor_total", color="forma_pagamento", barmode="stack", title="Valor por setor e forma de pagamento")
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            g2 = df.groupby(["setor"], dropna=False)["quantidade"].sum().reset_index()
            g2["setor"] = g2["setor"].fillna("(Sem setor)")
            fig2 = px.bar(g2, x="setor", y="quantidade", title="Quantidade por setor")
            st.plotly_chart(fig2, use_container_width=True)
        st.markdown("—")
        g3 = df.resample("D", on="carimbo_dt").agg(total=("valor_total","sum"), qtd=("id","count")).reset_index()
        c3, c4 = st.columns(2)
        with c3:
            fig3 = px.line(g3, x="carimbo_dt", y="total", markers=True, title="Evolução diária de valor")
            st.plotly_chart(fig3, use_container_width=True)
        with c4:
            fig4 = px.line(g3, x="carimbo_dt", y="qtd", markers=True, title="Evolução diária de registros")
            st.plotly_chart(fig4, use_container_width=True)
        st.markdown("—")
        g4 = df.groupby(["cliente"], dropna=False)["valor_total"].sum().reset_index().sort_values("valor_total", ascending=False).head(10)
        fig5 = px.bar(g4, x="cliente", y="valor_total", title="Top clientes por valor")
        st.plotly_chart(fig5, use_container_width=True)