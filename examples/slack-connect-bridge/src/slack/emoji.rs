//! Slack `:shortcode:` emoji → Unicode, so standard emoji render in Buzz.

use emojis::SkinTone;

/// Replace standard Slack emoji shortcodes with Unicode emoji. Custom
/// workspace emoji and unknown names stay as text; code spans and blocks are
/// left untouched. A shortcode must not touch a letter or digit on either
/// side or follow a `/`, so times (`10:30:45`) and URLs are not rewritten.
pub(crate) fn slack_emoji_to_unicode(input: &str) -> String {
    input
        .split("```")
        .enumerate()
        .map(|(block, part)| {
            if block % 2 == 1 {
                return part.to_owned();
            }
            part.split('`')
                .enumerate()
                .map(|(span, text)| {
                    if span % 2 == 1 {
                        text.to_owned()
                    } else {
                        convert_plain(text)
                    }
                })
                .collect::<Vec<_>>()
                .join("`")
        })
        .collect::<Vec<_>>()
        .join("```")
}

fn convert_plain(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find(':') {
        let (before, from_colon) = rest.split_at(open);
        output.push_str(before);
        let preceded = output
            .chars()
            .last()
            .is_some_and(|c| c.is_alphanumeric() || c == '/');
        match (!preceded).then(|| emoji_at(from_colon)).flatten() {
            Some((emoji, consumed)) => {
                output.push_str(emoji);
                rest = &from_colon[consumed..];
            }
            None => {
                output.push(':');
                rest = &from_colon[1..];
            }
        }
    }
    output.push_str(rest);
    output
}

/// A `:name:` (optionally followed by `:skin-tone-N:`) at the start of
/// `text`: the emoji and how many bytes it used.
fn emoji_at(text: &str) -> Option<(&'static str, usize)> {
    let (name, mut used) = shortcode(text)?;
    let mut emoji = emojis::get_by_shortcode(name)?;
    if let Some((tone, tone_used)) = shortcode(&text[used..]) {
        if let Some(toned) = skin_tone(tone).and_then(|tone| emoji.with_skin_tone(tone)) {
            emoji = toned;
            used += tone_used;
        }
    }
    let followed = text[used..]
        .chars()
        .next()
        .is_some_and(char::is_alphanumeric);
    (!followed).then(|| (emoji.as_str(), used))
}

/// The name in a leading `:name:` and the bytes it spans.
fn shortcode(text: &str) -> Option<(&str, usize)> {
    let body = text.strip_prefix(':')?;
    let end = body.find(':')?;
    let name = &body[..end];
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '+' | '-'));
    valid.then_some((name, end + 2))
}

fn skin_tone(name: &str) -> Option<SkinTone> {
    Some(match name {
        "skin-tone-2" => SkinTone::Light,
        "skin-tone-3" => SkinTone::MediumLight,
        "skin-tone-4" => SkinTone::Medium,
        "skin-tone-5" => SkinTone::MediumDark,
        "skin-tone-6" => SkinTone::Dark,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_standard_shortcodes() {
        assert_eq!(
            slack_emoji_to_unicode(":white_check_mark: Mark Column K :tada:"),
            "✅ Mark Column K 🎉"
        );
    }

    #[test]
    fn applies_slack_skin_tones() {
        assert_eq!(slack_emoji_to_unicode(":thumbsup::skin-tone-4:"), "👍🏽");
    }

    #[test]
    fn keeps_custom_and_unknown_shortcodes() {
        assert_eq!(slack_emoji_to_unicode(":rex: done :"), ":rex: done :");
    }

    #[test]
    fn leaves_code_times_and_urls_alone() {
        assert_eq!(
            slack_emoji_to_unicode("`:tada:` at 10:30:45 ```:tada:``` https://x.io/:tada: :tada:"),
            "`:tada:` at 10:30:45 ```:tada:``` https://x.io/:tada: 🎉"
        );
    }
}
