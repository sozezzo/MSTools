function Send-MSToolsMail {
<#
.SYNOPSIS
    Sends an email using the legacy Send-MailMessage cmdlet.

.DESCRIPTION
    This is the MSTools-compatible email function using the old PowerShell
    Send-MailMessage engine. MailKit is no longer required.

    Supports:
    - SSL or plain SMTP
    - STARTTLS (mapped to UseSSL)
    - CC, BCC
    - Attachments
    - HTML body or plain text
    - Optional authentication
    - Internal logging using Write-MSLog (if available)
    - Structured output
    - Backward compatibility with previous MailKit interface

.PARAMETER SmtpServer
    The SMTP server hostname or IP address.

.PARAMETER Port
    SMTP port (default 587).
    Typical values:
        25  = plain SMTP (internal relay)
        587 = STARTTLS (recommended)
        465 = SSL on connect

.PARAMETER From
    Sender email address.

.PARAMETER To
    One or more recipients.

.PARAMETER Cc
    Optional CC recipients.

.PARAMETER Bcc
    Optional BCC recipients.

.PARAMETER Subject
    Email subject.

.PARAMETER Body
    Email body content (HTML or plain text).

.PARAMETER IsBodyHtml
    If provided, body is treated as HTML.

.PARAMETER AttachmentPath
    Optional attachment file paths.

.PARAMETER Username
    Username for SMTP authentication.

.PARAMETER Password
    Password for SMTP authentication.

.PARAMETER SecurityOption
    Controls SSL usage:
        None        = no SSL
        Auto        = SSL except on port 25
        StartTls    = SSL
        SslOnConnect= SSL

.PARAMETER MailKitAssemblyPath
    Ignored in this version, kept only for interface compatibility.

.PARAMETER ThrowOnError
    If present, throw exception instead of returning structured error.

.OUTPUTS
    PSCustomObject with Success, ErrorMessage, Server, Port, etc.

.EXAMPLE
    Send-MSToolsMail -SmtpServer mail.local -Port 25 -From x@y -To a@b -Subject "hi" -Body "Working now"

#>

    [CmdletBinding()]
    Param(
        [Parameter(Mandatory)]
        [string]$SmtpServer,

        [int]$Port = 587,

        [Parameter(Mandatory)]
        [string]$From,

        [Parameter(Mandatory)]
        [string[]]$To,

        [string[]]$Cc,
        [string[]]$Bcc,

        [Parameter(Mandatory)]
        [string]$Subject,

        [Parameter(Mandatory)]
        [string]$Body,

        [switch]$IsBodyHtml,

        [string[]]$AttachmentPath,

        [string]$Username,
        [string]$Password,

        [ValidateSet('Auto','SslOnConnect','StartTls','None')]
        [string]$SecurityOption = 'Auto',

        # Ignored but kept for compatibility with older interface
        [string]$MailKitAssemblyPath,

        [switch]$ThrowOnError
    )

    # Internal helper to use Write-MSLog if available
    function Invoke-InternalLog {
        param(
            [string]$Level,
            [string]$Message
        )

        $writeLog = Get-Command -Name 'Write-MSLog' -ErrorAction SilentlyContinue

        if ($writeLog) {
            try {
                Write-MSLog -Level $Level -Message $Message
            }
            catch {
                Write-Verbose "Write-MSLog failed: $Message"
            }
        }
        else {
            Write-Verbose $Message
        }
    }

    # Measure duration
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $errorMessage = $null
    $success = $false

    try {
        # Informational logging
        Invoke-InternalLog -Level 'Info' -Message "Preparing email to '$($To -join ",")' via $SmtpServer : $Port."

        # -------------------------
        # SSL / TLS DECISION LOGIC
        # -------------------------
        # "None" always means plain
        # "Auto" = plain if port 25, otherwise SSL
        # "StartTls" and "SslOnConnect" both become SSL in Send-MailMessage
        # -------------------------
        $useSsl = $false
        switch ($SecurityOption) {
            'None' {
                $useSsl = $false
            }
            'Auto' {
                if ($Port -eq 25) {
                    $useSsl = $false
                }
                else {
                    $useSsl = $true
                }
            }
            'StartTls' {
                $useSsl = $true
            }
            'SslOnConnect' {
                $useSsl = $true
            }
        }

        # Authentication (optional)
        $credential = $null
        if ($Username -and $Password) {
            $securePassword = ConvertTo-SecureString $Password -AsPlainText -Force
            $credential = New-Object System.Management.Automation.PSCredential ($Username, $securePassword)
        }

        # Build Send-MailMessage parameter table
        $mailParams = @{
            SmtpServer = $SmtpServer
            Port       = $Port
            From       = $From
            To         = $To
            Subject    = $Subject
            Body       = $Body
            UseSsl     = $useSsl
        }

        # Optional parameters
        if ($Cc)             { $mailParams.Cc          = $Cc }
        if ($Bcc)            { $mailParams.Bcc         = $Bcc }
        if ($AttachmentPath) { $mailParams.Attachments = $AttachmentPath }
        if ($IsBodyHtml)     { $mailParams.BodyAsHtml  = $true }
        if ($credential)     { $mailParams.Credential  = $credential }

        Invoke-InternalLog -Level 'Info' -Message "Sending email via Send-MailMessage (UseSsl=$useSsl) to '$($To -join ",")'."

        # ---- Actual SMTP send ----
        Send-MailMessage @mailParams

        # Success
        $success = $true
        Invoke-InternalLog -Level 'Info' -Message "Email sent successfully to '$($To -join ",")'."
    }
    catch {
        $errorMessage = $_.Exception.Message
        $success = $false

        Invoke-InternalLog -Level 'Error' -Message "Failed to send email: $errorMessage"
        Write-Error "Send-MSToolsMail failed: $errorMessage"

        if ($ThrowOnError) { throw }
    }
    finally {
        $stopwatch.Stop()
    }

    # -------------------------
    # Output structured object
    # -------------------------
    [pscustomobject]@{
        Success      = $success
        ErrorMessage = $errorMessage
        SmtpServer   = $SmtpServer
        Port         = $Port
        From         = $From
        To           = $To -join ','
        Subject      = $Subject
        DurationMs   = [math]::Round($stopwatch.Elapsed.TotalMilliseconds, 2)
        Timestamp    = (Get-Date)
    }
}
