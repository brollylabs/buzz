//! Minimal Slack Events/Web API client for the bridge.

mod api;
mod webhook;

use std::collections::HashMap;

pub(crate) use api::{SlackClient, UploadFile};
pub(crate) use webhook::{
    run_webhook_server, SlackDelivery, SlackEvent, SlackFile, WebhookControl, WebhookServerState,
};

/// Slack user IDs mentioned as `<@U…>` or `<@U…|label>`, in order, once each.
pub(crate) fn slack_user_mentions(input: &str) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for (start, _) in input.match_indices("<@") {
        let rest = &input[start + 2..];
        let Some(close) = rest.find('>') else {
            break;
        };
        let id = rest[..close].split('|').next().unwrap_or_default();
        if !id.is_empty() && !ids.iter().any(|seen| seen == id) {
            ids.push(id.to_owned());
        }
    }
    ids
}

/// Convert the subset of Slack mrkdwn that would otherwise be unreadable in
/// Buzz. Unknown control tokens stay visible instead of being discarded.
/// User mentions become `@name` using `names` (Slack user ID → display name),
/// then the mention's own label, then the raw ID.
pub(crate) fn slack_mrkdwn_to_markdown(input: &str, names: &HashMap<String, String>) -> String {
    let decoded = input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">");
    let mut output = String::with_capacity(decoded.len());
    let mut rest = decoded.as_str();

    while let Some(open) = rest.find('<') {
        output.push_str(&rest[..open]);
        let after_open = &rest[open + 1..];
        let Some(close) = after_open.find('>') else {
            output.push_str(&rest[open..]);
            return output;
        };
        let token = &after_open[..close];
        output.push_str(&convert_control_token(token, names));
        rest = &after_open[close + 1..];
    }
    output.push_str(rest);
    output
}

fn convert_control_token(token: &str, names: &HashMap<String, String>) -> String {
    if let Some(mention) = token.strip_prefix('@') {
        let (user_id, label) = mention
            .split_once('|')
            .map_or((mention, None), |(id, label)| (id, Some(label)));
        let name = names
            .get(user_id)
            .map(String::as_str)
            .or(label)
            .unwrap_or(user_id);
        return format!("@{}", escape_markdown_label(name));
    }
    if let Some(channel) = token.strip_prefix('#') {
        let label = channel.split_once('|').map_or(channel, |(_, label)| label);
        return format!("#{label}");
    }
    if let Some(command) = token.strip_prefix('!') {
        let label = command
            .split_once('|')
            .map_or(command, |(_, label)| label.trim_start_matches('@'));
        return format!("@{label}");
    }
    if let Some((url, label)) = token.split_once('|') {
        if url.starts_with("http://") || url.starts_with("https://") {
            return format!("[{label}]({url})");
        }
        if let Some(address) = url.strip_prefix("mailto:") {
            return format!("[{label}](mailto:{address})");
        }
    }
    if token.starts_with("http://") || token.starts_with("https://") {
        return token.to_owned();
    }
    if let Some(address) = token.strip_prefix("mailto:") {
        return address.to_owned();
    }
    format!("<{token}>")
}

/// Make an untrusted display name safe in a Buzz Markdown bold span.
pub(crate) fn escape_markdown_label(input: &str) -> String {
    input
        .chars()
        .flat_map(|character| match character {
            '\\' | '*' | '_' | '[' | ']' | '`' => vec!['\\', character],
            '\r' | '\n' => vec![' '],
            other => vec![other],
        })
        .take(160)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_slack_links_channels_and_mentions() {
        assert_eq!(
            slack_mrkdwn_to_markdown(
                "See <https://example.com|the docs> in <#C12345678|project-x> with <@U12345678> &amp; <!here>",
                &HashMap::new()
            ),
            "See [the docs](https://example.com) in #project-x with @U12345678 & @here"
        );
    }

    #[test]
    fn resolves_user_mentions_to_names() {
        let names = HashMap::from([("U1".to_owned(), "Ram *S*".to_owned())]);
        assert_eq!(
            slack_mrkdwn_to_markdown("<@U1> and <@U2|bob> and <@U3>", &names),
            "@Ram \\*S\\* and @bob and @U3"
        );
    }

    #[test]
    fn lists_user_mentions_once_in_order() {
        assert_eq!(
            slack_user_mentions("<@U2> hi <@U1|x> <@U2> <#C1|general> <!here>"),
            vec!["U2".to_owned(), "U1".to_owned()]
        );
    }

    #[test]
    fn malformed_control_token_stays_visible() {
        assert_eq!(
            slack_mrkdwn_to_markdown("before <not-closed", &HashMap::new()),
            "before <not-closed"
        );
        assert_eq!(
            slack_mrkdwn_to_markdown("before <unknown> after", &HashMap::new()),
            "before <unknown> after"
        );
    }

    #[test]
    fn escapes_untrusted_display_names() {
        assert_eq!(
            escape_markdown_label("*Mallory*\n`admin`"),
            "\\*Mallory\\* \\`admin\\`"
        );
    }
}
