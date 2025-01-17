use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{BorrowToSql, IsNull};
use crate::{Column, Error, Portal, Row, Statement};
use bytes::{BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use futures_util::{ready, Stream};
use log::{debug, log_enabled, Level};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::{
    CommandCompleteBody, Message, ParameterDescriptionBody, RowDescriptionBody,
};
use postgres_protocol::message::frontend;
use postgres_types::Format;
use std::fmt;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

struct BorrowToSqlParamsDebug<'a, T>(&'a [T]);

impl<'a, T> fmt::Debug for BorrowToSqlParamsDebug<'a, T>
where
    T: BorrowToSql,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|x| x.borrow_to_sql()))
            .finish()
    }
}

pub async fn query<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if log_enabled!(Level::Debug) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode(client, &statement, params)?
    } else {
        encode(client, &statement, params)?
    };

    let responses = start(client, buf).await?;

    Ok(RowStream {
        statement: None,
        responses,
        rows_affected: None,
        command_tag: None,
        status: None,
        output_format: Format::Binary,
        parameter_description: None,
        _p: PhantomPinned,
    })
}

pub async fn query_txt<S, I>(
    client: &Arc<InnerClient>,
    query: &str,
    params: I,
) -> Result<RowStream, Error>
where
    S: AsRef<str>,
    I: IntoIterator<Item = Option<S>>,
    I::IntoIter: ExactSizeIterator,
{
    let params = params.into_iter();

    let buf = client.with_buf(|buf| {
        // Prepare
        frontend::parse("", query, std::iter::empty(), buf).map_err(Error::encode)?;

        // Bind, pass params as text, retrieve as binary
        match frontend::bind(
            "",                 // empty string selects the unnamed portal
            "",                 // unnamed prepared statement
            std::iter::empty(), // all parameters use the default format (text)
            params,
            |param, buf| match param {
                Some(param) => {
                    buf.put_slice(param.as_ref().as_bytes());
                    Ok(postgres_protocol::IsNull::No)
                }
                None => Ok(postgres_protocol::IsNull::Yes),
            },
            Some(0), // all text
            buf,
        ) {
            Ok(()) => Ok(()),
            Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, 0)),
            Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
        }?;

        // Describe
        frontend::describe(b'S', "", buf).map_err(Error::encode)?;

        // Execute
        frontend::execute("", 0, buf).map_err(Error::encode)?;

        // Sync
        frontend::sync(buf);

        Ok(buf.split().freeze())
    })?;

    // now read the responses
    let responses = start(client, buf).await?;

    Ok(RowStream {
        parameter_description: None,
        statement: None,
        responses,
        command_tag: None,
        status: None,
        output_format: Format::Text,
        _p: PhantomPinned,
        rows_affected: None,
    })
}

pub async fn query_portal(
    client: &InnerClient,
    portal: &Portal,
    max_rows: i32,
) -> Result<RowStream, Error> {
    let buf = client.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(RowStream {
        parameter_description: None,
        statement: Some(portal.statement().clone()),
        responses,
        rows_affected: None,
        command_tag: None,
        status: None,
        output_format: Format::Binary,
        _p: PhantomPinned,
    })
}

/// Extract the number of rows affected from [`CommandCompleteBody`].
pub fn extract_row_affected(body: &CommandCompleteBody) -> Result<u64, Error> {
    let rows = body
        .tag()
        .map_err(Error::parse)?
        .rsplit(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap_or(0);
    Ok(rows)
}

pub async fn execute<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<u64, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if log_enabled!(Level::Debug) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode(client, &statement, params)?
    } else {
        encode(client, &statement, params)?
    };

    let mut responses = start(client, buf).await?;

    let mut rows = 0;
    loop {
        match responses.next().await? {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                rows = extract_row_affected(&body)?;
            }
            Message::EmptyQueryResponse => rows = 0,
            Message::ReadyForQuery(_) => return Ok(rows),
            m => return Err(Error::unexpected_message(m)),
        }
    }
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next().await? {
            Message::ParseComplete => {}
            Message::BindComplete | Message::ReadyForQuery(_) => return Ok(responses),
            m => return Err(Error::unexpected_message(m)),
        }
    }
}

fn make_statement(
    parameter_description: ParameterDescriptionBody,
    row_description: Option<RowDescriptionBody>,
) -> Result<Statement, Error> {
    let mut parameters = vec![];
    let mut it = parameter_description.parameters();

    while let Some(oid) = it.next().map_err(Error::parse).unwrap() {
        let type_ = crate::prepare::get_type(oid);
        parameters.push(type_);
    }

    let mut columns = Vec::new();

    if let Some(row_description) = row_description {
        let mut it = row_description.fields();

        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = crate::prepare::get_type(field.type_oid());
            let column = Column {
                name: field.name().to_string(),
                table_oid: Some(field.table_oid()).filter(|n| *n != 0),
                column_id: Some(field.column_id()).filter(|n| *n != 0),
                r#type: type_,
            };

            columns.push(column);
        }
    }

    Ok(Statement::unnamed(parameters, columns))
}

pub fn encode<P, I>(client: &InnerClient, statement: &Statement, params: I) -> Result<Bytes, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    client.with_buf(|buf| {
        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::sync(buf);

        Ok(buf.split().freeze())
    })
}

pub fn encode_bind<P, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let param_types = statement.params();
    let params = params.into_iter();

    if param_types.len() != params.len() {
        return Err(Error::parameters(params.len(), param_types.len()));
    }

    let (param_formats, params): (Vec<_>, Vec<_>) = params
        .zip(param_types.iter())
        .map(|(p, ty)| (p.borrow_to_sql().encode_format(ty) as i16, p))
        .unzip();

    let params = params.into_iter();

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        param_formats,
        params.zip(param_types).enumerate(),
        |(idx, (param, ty)), buf| match param.borrow_to_sql().to_sql_checked(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        buf,
    );
    match r {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
    }
}

pin_project! {
    /// A stream of table rows.
    pub struct RowStream {
        statement: Option<Statement>,
        responses: Responses,
        rows_affected: Option<u64>,
        command_tag: Option<String>,
        output_format: Format,
        status: Option<u8>,
        parameter_description: Option<ParameterDescriptionBody>,

        #[pin]
        _p: PhantomPinned,
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        loop {
            match ready!(this.responses.poll_next(cx)?) {
                Message::DataRow(body) => {
                    return Poll::Ready(Some(Ok(Row::new(
                        this.statement.as_ref().unwrap().clone(),
                        body,
                        *this.output_format,
                    )?)))
                }
                Message::CommandComplete(body) => {
                    *this.rows_affected = Some(extract_row_affected(&body)?);

                    if let Ok(tag) = body.tag() {
                        *this.command_tag = Some(tag.to_string());
                    }
                }
                Message::ParameterDescription(body) => {
                    *this.parameter_description = Some(body);
                }
                Message::NoData => {
                    *this.statement = Some(make_statement(
                        this.parameter_description.take().unwrap(),
                        None,
                    )?);
                }
                Message::RowDescription(body) => {
                    *this.statement = Some(make_statement(
                        this.parameter_description.take().unwrap(),
                        Some(body),
                    )?);
                }
                Message::EmptyQueryResponse | Message::PortalSuspended => {}
                Message::ReadyForQuery(status) => {
                    *this.status = Some(status.status());
                    return Poll::Ready(None);
                }
                m => return Poll::Ready(Some(Err(Error::unexpected_message(m)))),
            }
        }
    }
}

impl RowStream {
    /// Returns the number of rows affected by the query.
    ///
    /// This function will return `None` until the stream has been exhausted.
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }

    /// Returns the command tag of this query.
    ///
    /// This is only available after the stream has been exhausted.
    pub fn command_tag(&self) -> Option<String> {
        self.command_tag.clone()
    }

    /// Returns if the connection is ready for querying, with the status of the connection.
    ///
    /// This might be available only after the stream has been exhausted.
    pub fn ready_status(&self) -> Option<u8> {
        self.status
    }
}
