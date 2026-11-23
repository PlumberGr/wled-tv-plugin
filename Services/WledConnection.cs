using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Jellyfin.Plugin.WledTv.Services;

/// <summary>
/// A WLED WebSocket connection plus the colour protocol.  Sends the on/brightness
/// message once per connection, then per-frame colour updates as WLED "seg.i"
/// individual-LED messages — batched into 54-LED chunks for ESP8266, or a single
/// message when batching is off.  Batch order is rotated per frame so that if the
/// controller drops a message it is not always the same segment.
/// </summary>
internal sealed class WledConnection : IDisposable
{
    private const int BatchSize = 54;

    private readonly string _url;
    private ClientWebSocket? _ws;
    private bool _ledsOn;
    private int _frame;

    public WledConnection(string url)
    {
        _url = url;
    }

    public bool IsOpen => _ws != null && _ws.State == WebSocketState.Open;

    public async Task<bool> EnsureConnectedAsync(CancellationToken ct)
    {
        if (IsOpen) return true;
        Dispose();
        var ws = new ClientWebSocket();
        ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);
        _ledsOn = false;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(5));
            await ws.ConnectAsync(new Uri(_url), cts.Token).ConfigureAwait(false);
            _ws = ws;
            _ = Task.Run(() => DrainAsync(ws));
            return true;
        }
        catch
        {
            ws.Dispose();
            return false;
        }
    }

    public async Task SendColorsAsync(byte[] colors, int brightness, bool batch, CancellationToken ct)
    {
        if (!IsOpen) return;

        if (!_ledsOn)
        {
            await SendTextAsync("{\"on\":true,\"bri\":" + Math.Clamp(brightness, 0, 255) + "}", ct).ConfigureAwait(false);
            _ledsOn = true;
        }

        int leds = colors.Length / 3;
        if (leds <= 0) return;

        if (!batch)
        {
            await SendTextAsync(BuildMessage(colors, 0, leds, false), ct).ConfigureAwait(false);
            return;
        }

        var starts = new List<int>();
        for (int start = 0; start < leds; start += BatchSize)
        {
            int batchStart = (start + BatchSize > leds && leds >= BatchSize) ? leds - BatchSize : start;
            starts.Add(batchStart);
        }

        int n = starts.Count;
        int off = n > 0 ? (_frame % n) : 0;
        for (int k = 0; k < n; k++)
        {
            int bs = starts[(off + k) % n];
            int count = Math.Min(BatchSize, leds - bs);
            await SendTextAsync(BuildMessage(colors, bs, count, bs != 0), ct).ConfigureAwait(false);
        }
        _frame++;
    }

    public async Task SendOffAsync(CancellationToken ct)
    {
        if (!IsOpen) return;
        try { await SendTextAsync("{\"on\":false}", ct).ConfigureAwait(false); }
        catch { /* best effort */ }
    }

    private static string BuildMessage(byte[] colors, int start, int count, bool includeStart)
    {
        var sb = new StringBuilder(count * 9 + 24);
        sb.Append("{\"seg\":[{\"i\":[");
        if (includeStart)
        {
            sb.Append(start);
            sb.Append(',');
        }
        for (int i = 0; i < count; i++)
        {
            int p = (start + i) * 3;
            if (i > 0) sb.Append(',');
            sb.Append('"');
            AppendHex(sb, colors[p]);
            AppendHex(sb, colors[p + 1]);
            AppendHex(sb, colors[p + 2]);
            sb.Append('"');
        }
        sb.Append("]}]}");
        return sb.ToString();
    }

    private static void AppendHex(StringBuilder sb, byte b)
    {
        const string H = "0123456789ABCDEF";
        sb.Append(H[b >> 4]);
        sb.Append(H[b & 0xF]);
    }

    // WLED pushes its state JSON back on the same socket.  If nobody reads it the
    // receive buffer fills, the connection deadlocks and SendAsync hangs forever
    // (LEDs freeze ~2.5 min into playback).  Read and discard everything.
    private static async Task DrainAsync(ClientWebSocket ws)
    {
        var buf = new byte[4096];
        try
        {
            while (ws.State == WebSocketState.Open)
            {
                var r = await ws.ReceiveAsync(new ArraySegment<byte>(buf), CancellationToken.None).ConfigureAwait(false);
                if (r.MessageType == WebSocketMessageType.Close)
                {
                    ws.Abort();
                    return;
                }
            }
        }
        catch { /* socket closed or aborted */ }
    }

    // Sends with a 2 s timeout; a stuck send aborts the socket so the next frame
    // reconnects instead of hanging forever.
    private async Task SendTextAsync(string json, CancellationToken ct)
    {
        var ws = _ws!;
        var bytes = Encoding.ASCII.GetBytes(json);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, cts.Token).ConfigureAwait(false);
        }
        catch when (!ct.IsCancellationRequested)
        {
            try { ws.Abort(); } catch { /* ignore */ }
            Dispose();
        }
    }

    public void Dispose()
    {
        try { _ws?.Abort(); _ws?.Dispose(); } catch { /* ignore */ }
        _ws = null;
        _ledsOn = false;
    }
}
